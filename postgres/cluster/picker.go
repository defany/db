package cluster

import (
	"math"
	"time"

	"golang.yandex/hasql/v2"
)

type NodeInfo struct {
	// ClusterRole contains determined node's role in cluster
	ClusterRole hasql.NodeRole
	// NetworkLatency stores time that has been spent to send check request
	// and receive response from server
	NetworkLatency time.Duration
	// ReplicaLag represents how far behind is data on standby
	// in comparison to primary. As determination of real replication
	// lag is a tricky task and value type vary from one DBMS to another
	// (e.g. bytes count lag, time delta lag etc.) this field contains
	// abstract value for sorting purposes only
	ReplicaLag int
	// Application stores application name used in session
	Application string
}

func (n NodeInfo) Latency() time.Duration {
	return n.NetworkLatency
}

func (n NodeInfo) ReplicationLag() int {
	return n.ReplicaLag
}

func (n NodeInfo) Role() hasql.NodeRole {
	return n.ClusterRole
}

func (n NodeInfo) ApplicationName() string {
	return n.Application
}

// compile time guard
var (
	_ hasql.NodePicker[WrappedPool] = (*CustomPicker[WrappedPool])(nil)
	_ hasql.NodeInfoProvider        = (NodeInfo{})
)

// CustomPickerOptions holds options to pick nodes
type CustomPickerOptions struct {
	MaxLag           int
	Priority         map[string]int32
	NodeRolePriority map[string]map[string]int32
	Retries          int
}

// CustomPickerOption func apply option to CustomPickerOptions
type CustomPickerOption func(*CustomPickerOptions)

// CustomPickerMaxLag specifies max lag for which node can be used
func CustomPickerMaxLag(n int) CustomPickerOption {
	return func(o *CustomPickerOptions) {
		o.MaxLag = n
	}
}

// NewCustomPicker creates new node picker
func NewCustomPicker[T WrappedPool](opts ...CustomPickerOption) *CustomPicker[WrappedPool] {
	options := CustomPickerOptions{}
	for _, o := range opts {
		o(&options)
	}
	return &CustomPicker[WrappedPool]{opts: options}
}

// CustomPicker holds node picker options
type CustomPicker[T WrappedPool] struct {
	opts CustomPickerOptions
}

// PickNode used to return specific node
func (p *CustomPicker[T]) PickNode(cnodes []hasql.CheckedNode[T]) hasql.CheckedNode[T] {
	return cnodes[0]
}

func (p *CustomPicker[T]) getPriority(n hasql.CheckedNode[T]) int32 {
	rname := "primary"
	switch n.Info.Role() {
	case hasql.NodeRolePrimary:
		rname = "primary"
	case hasql.NodeRoleStandby:
		rname = "standby"
	}
	var prio int32
	nodeRolePrio, ok := p.opts.NodeRolePriority[n.Node.String()]
	if ok && nodeRolePrio != nil {
		if prio, ok = nodeRolePrio[rname]; ok {
			return prio
		}
	}
	if prio, ok := p.opts.Priority[n.Node.String()]; ok {
		return prio
	}
	return math.MaxInt32 // Default to higest priority
}

// CompareNodes used to sort nodes
func (p *CustomPicker[T]) CompareNodes(a, b hasql.CheckedNode[T]) int {
	// Get replication lag values
	aLag := a.Info.(interface{ ReplicationLag() int }).ReplicationLag()
	bLag := b.Info.(interface{ ReplicationLag() int }).ReplicationLag()

	// First check that lag lower then MaxLag
	if aLag > p.opts.MaxLag && bLag > p.opts.MaxLag {
		return 0 // both are equal
	}

	// If one node exceeds MaxLag and the other doesn't, prefer the one that doesn't
	if aLag > p.opts.MaxLag {
		return 1 // b is better
	}
	if bLag > p.opts.MaxLag {
		return -1 // a is better
	}

	// Get node priorities
	aPrio := p.getPriority(a)
	bPrio := p.getPriority(b)

	// if both priority equals
	if aPrio == bPrio {
		// First compare by replication lag
		if aLag < bLag {
			return -1
		}
		if aLag > bLag {
			return 1
		}
		// If replication lag is equal, compare by latency
		aLatency := a.Info.(interface{ Latency() time.Duration }).Latency()
		bLatency := b.Info.(interface{ Latency() time.Duration }).Latency()

		if aLatency < bLatency {
			return -1
		}
		if aLatency > bLatency {
			return 1
		}

		// If lag and latency is equal
		return 0
	}

	// If priorities are different, prefer the node with lower priority value
	if aPrio < bPrio {
		return -1
	}

	return 1
}

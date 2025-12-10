package cluster

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"math"
	"time"

	"golang.yandex/hasql/v2"
)

type RetryFunc func(ctx context.Context, retryCount int, err error) bool

func RetryNever(ctx context.Context, _ int, err error) bool {
	return false
}

func RetryErrorWithLogger() RetryFunc {
	return func(ctx context.Context, retryCount int, err error) bool {
		retry := RetryError(ctx, retryCount, err)
		if err != nil {
			// log := logger.Extract(ctx)
			// log.Error(ctx, fmt.Sprintf("cluster retry error, count %d", retryCount), sl.ErrAttr(err))
		}
		return retry
	}
}

func RetryError(ctx context.Context, _ int, err error) bool {
	// add pgconn.SafeToRetry check
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	case errors.Is(err, sql.ErrNoRows), errors.Is(err, sql.ErrTxDone):
		return false
	case err == nil:
		return false
	default:
		return true
	}
}

func RetryBackoff(retryNum int, minWait time.Duration, maxWait time.Duration) RetryFunc {
	return func(ctx context.Context, retryCount int, err error) bool {
		if err != nil && retryCount < retryNum {
			time.Sleep(RandomInterval(minWait, maxWait))
			return true
		}
		return false
	}
}

func RetryErrorBackoff(retryNum int, minWait time.Duration, maxWait time.Duration) RetryFunc {
	return func(ctx context.Context, retryCount int, err error) bool {
		switch {
		case errors.Is(err, context.Canceled):
			return false
		case errors.Is(err, context.DeadlineExceeded):
			return false
		case errors.Is(err, sql.ErrNoRows):
			return false
		case errors.Is(err, sql.ErrTxDone):
			return false
		case err == nil:
			return false
		default:
			if retryCount < retryNum {
				time.Sleep(RandomInterval(minWait, maxWait))
				return true
			}
			return false
		}
	}
}

func NewClusterOptions(opts ...ClusterOption) ClusterOptions {
	options := ClusterOptions{
		Context:            context.Background(),
		RetryFunc:          RetryNever,
		NodeStateCriterion: hasql.Primary,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// ClusterOptions contains cluster specific options.
type ClusterOptions struct {
	NodeChecker        hasql.NodeChecker
	NodePicker         hasql.NodePicker[WrappedPool]
	NodeDiscoverer     hasql.NodeDiscoverer[WrappedPool]
	Options            []hasql.ClusterOpt[WrappedPool]
	Context            context.Context
	RetryFunc          RetryFunc
	NodePriority       map[string]int32
	NodeRolePriority   map[string]map[string]int32
	NodeStateCriterion hasql.NodeStateCriterion
}

// ClusterOption apply cluster options to ClusterOptions.
type ClusterOption func(*ClusterOptions)

// WithClusterNodeChecker pass hasql.NodeChecker to cluster options.
func WithClusterNodeChecker(c hasql.NodeChecker) ClusterOption {
	return func(o *ClusterOptions) {
		o.NodeChecker = c
	}
}

// WithClusterNodePicker pass hasql.NodePicker to cluster options.
func WithClusterNodePicker(p hasql.NodePicker[WrappedPool]) ClusterOption {
	return func(o *ClusterOptions) {
		o.NodePicker = p
	}
}

// WithClusterNodeDiscoverer pass hasql.NodeDiscoverer to cluster options.
func WithClusterNodeDiscoverer(d hasql.NodeDiscoverer[WrappedPool]) ClusterOption {
	return func(o *ClusterOptions) {
		o.NodeDiscoverer = d
	}
}

// WithClusterRetryFunc provide func to check is error is suitable for retry.
func WithClusterRetries(fn RetryFunc) ClusterOption {
	return func(o *ClusterOptions) {
		o.RetryFunc = fn
	}
}

// WithClusterContext pass context.Context to cluster options and used for checks
func WithClusterContext(ctx context.Context) ClusterOption {
	return func(o *ClusterOptions) {
		o.Context = ctx
	}
}

// WithClusterOptions pass hasql.ClusterOpt
func WithClusterOptions(opts ...hasql.ClusterOpt[WrappedPool]) ClusterOption {
	return func(o *ClusterOptions) {
		o.Options = append(o.Options, opts...)
	}
}

// WithClusterNodeStateCriterion pass default hasql.NodeStateCriterion
func WithClusterNodeStateCriterion(c hasql.NodeStateCriterion) ClusterOption {
	return func(o *ClusterOptions) {
		o.NodeStateCriterion = c
	}
}

type ClusterNode struct {
	Name     string
	DB       WrappedPool
	Priority int32
}

// WithClusterNodeRolePriority create cluster with additional node role priorities
func WithClusterNodeRolePriority(p map[string]map[string]int32) ClusterOption {
	return func(o *ClusterOptions) {
		o.NodeRolePriority = make(map[string]map[string]int32, len(p))
		maps.Copy(o.NodeRolePriority, p)
	}
}

// WithPickerNodeRolePriority create cluster with additional node role priorities
func WithPickerNodeRolePriority(p map[string]map[string]int32) CustomPickerOption {
	return func(o *CustomPickerOptions) {
		o.NodeRolePriority = make(map[string]map[string]int32, len(p))
		maps.Copy(o.NodeRolePriority, p)
	}
}

// WithClusterNodes create cluster with static NodeDiscoverer and priority
func WithClusterNodes(cns ...ClusterNode) ClusterOption {
	return func(o *ClusterOptions) {
		nodes := make([]*hasql.Node[WrappedPool], 0, len(cns))
		if o.NodePriority == nil {
			o.NodePriority = make(map[string]int32, len(cns))
		}
		for _, cn := range cns {
			nodes = append(nodes, hasql.NewNode(cn.Name, cn.DB))
			if cn.Priority == 0 {
				cn.Priority = math.MaxInt32
			}
			o.NodePriority[cn.Name] = cn.Priority
		}
		o.NodeDiscoverer = hasql.NewStaticNodeDiscoverer(nodes...)
	}
}

type nodeStateCriterionKey struct{}

// NodeStateCriterion inject hasql.NodeStateCriterion to context
func NodeStateCriterion(ctx context.Context, c hasql.NodeStateCriterion) context.Context {
	return context.WithValue(ctx, nodeStateCriterionKey{}, c)
}

func (c *Cluster) getNodeStateCriterion(ctx context.Context) hasql.NodeStateCriterion {
	if v, ok := ctx.Value(nodeStateCriterionKey{}).(hasql.NodeStateCriterion); ok {
		return v
	}
	return c.options.NodeStateCriterion
}

package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	hasql "golang.yandex/hasql/v2"
)

func TestCustomNodePicker(t *testing.T) {
	t.Run("pick_node", func(t *testing.T) {
		nodes := []hasql.CheckedNode[WrappedPool]{
			{
				Node: hasql.NewNode("dc1-master", (WrappedPool)(nil)),
			},
			{
				Node: hasql.NewNode("dc2-slave", (WrappedPool)(nil)),
			},
			{
				Node: hasql.NewNode("dc1-replica", (WrappedPool)(nil)),
			},
			{
				Node: hasql.NewNode("dc2-replica", (WrappedPool)(nil)),
			},
		}

		np := new(CustomPicker[WrappedPool])

		pickedNodes := make(map[string]struct{})
		for range 100 {
			pickedNodes[np.PickNode(nodes).Node.String()] = struct{}{}
		}

		expectedNodes := map[string]struct{}{
			"dc1-master": {},
		}
		assert.Equal(t, expectedNodes, pickedNodes)
	})

	t.Run("compare_nodes priority", func(t *testing.T) {
		dc1Master := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-master", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRolePrimary,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc2Slave := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-slave", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc1Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc2Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		np := new(CustomPicker[WrappedPool])
		np.opts.MaxLag = 100
		np.opts.Priority = map[string]int32{
			"dc1-replica": 1,
			"dc1-master":  2,
			"dc2-slave":   99,
			"dc2-replica": 98,
		}
		assert.Equal(t, -1, np.CompareNodes(dc1Master, dc2Slave))
		assert.Equal(t, -1, np.CompareNodes(dc1Replica, dc2Replica))
		assert.Equal(t, 1, np.CompareNodes(dc1Master, dc1Replica))
		assert.Equal(t, 1, np.CompareNodes(dc2Slave, dc2Replica))
	})

	t.Run("compare_nodes node_role_priority standard", func(t *testing.T) {
		dc1Master := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-master", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRolePrimary,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc2Slave := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-slave", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc1Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc2Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		np := new(CustomPicker[WrappedPool])
		np.opts.MaxLag = 100

		WithPickerNodeRolePriority(
			map[string]map[string]int32{
				"dc1-master": {
					"primary": 2,
					"standby": 99,
				},
				"dc1-replica": {
					"standby": 1,
				},
				"dc2-replica": {
					"standby": 1,
				},
				"dc2-slave": {
					"primary": 2,
					"standby": 99,
				},
			})(&np.opts)

		assert.Equal(t, -1, np.CompareNodes(dc1Master, dc2Slave))
		assert.Equal(t, 0, np.CompareNodes(dc1Replica, dc2Replica))
		assert.Equal(t, 1, np.CompareNodes(dc1Master, dc1Replica))
		assert.Equal(t, 1, np.CompareNodes(dc2Slave, dc2Replica))
	})

	t.Run("compare_nodes node_role_priority lag failover", func(t *testing.T) {
		dc1Master := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-master", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleUnknown,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     1000,
			},
		}

		dc2Slave := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-slave", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc1Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc2Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		np := new(CustomPicker[WrappedPool])
		np.opts.MaxLag = 100

		WithPickerNodeRolePriority(
			map[string]map[string]int32{
				"dc1-master": {
					"primary": 2,
					"standby": 99,
				},
				"dc1-replica": {
					"standby": 1,
				},
				"dc2-replica": {
					"standby": 1,
				},
				"dc2-slave": {
					"primary": 2,
					"standby": 99,
				},
			})(&np.opts)

		assert.Equal(t, 1, np.CompareNodes(dc1Master, dc2Slave))
		assert.Equal(t, 0, np.CompareNodes(dc1Replica, dc2Replica))
		assert.Equal(t, 1, np.CompareNodes(dc1Master, dc1Replica))
		assert.Equal(t, 1, np.CompareNodes(dc2Slave, dc2Replica))
	})

	t.Run("compare_nodes node_role_priority dr failover", func(t *testing.T) {
		dc1Master := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-master", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleUnknown,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     1000,
			},
		}

		dc2Slave := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-slave", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc1Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc1-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		dc2Replica := hasql.CheckedNode[WrappedPool]{
			Node: hasql.NewNode("dc2-replica", (WrappedPool)(nil)),
			Info: NodeInfo{
				ClusterRole:    hasql.NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     0,
			},
		}

		np := new(CustomPicker[WrappedPool])
		np.opts.MaxLag = 100

		WithPickerNodeRolePriority(
			map[string]map[string]int32{
				"dc1-master": {
					"primary": 2,
					"standby": 99,
				},
				"dc1-replica": {
					"standby": 1,
				},
				"dc2-replica": {
					"standby": 1,
				},
				"dc2-slave": {
					"primary": 2,
					"standby": 99,
				},
			})(&np.opts)

		assert.Equal(t, 1, np.CompareNodes(dc1Master, dc2Slave))
		assert.Equal(t, 0, np.CompareNodes(dc1Replica, dc2Replica))
		assert.Equal(t, 1, np.CompareNodes(dc1Master, dc1Replica))
		assert.Equal(t, 1, np.CompareNodes(dc2Slave, dc2Replica))
	})
}

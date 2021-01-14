package txshard

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestState_RunLoop_NodeEvent(t *testing.T) {
	table := []struct {
		name string

		event NodeEvent

		selfNodeID        NodeID
		selfLastPartition PartitionID

		stateBefore func(s *state)
		stateAfter  func(s *state)

		output runLoopOutput
	}{
		{
			name: "add-node-same-as-self",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				NodeID:        7,
				LastPartition: 3,
				Revision:      100,
			},
			selfNodeID: 7,
			stateBefore: func(s *state) {
				s.leaseID = 555
			},
			stateAfter: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					7: {
						ID:            7,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/0",
						Value:   "7",
						LeaseID: 555,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/1",
						Value:   "7",
						LeaseID: 555,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/2",
						Value:   "7",
						LeaseID: 555,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/3",
						Value:   "7",
						LeaseID: 555,
					},
				},
			},
		},
		{
			name: "add-two-nodes",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				LastPartition: 1,
				NodeID:        2,
				Revision:      200,
			},
			selfNodeID: 1,
			stateBefore: func(s *state) {
				s.leaseID = 512
				s.nodeMap = map[NodeID]Node{
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/2",
						Value:   "1",
						LeaseID: 512,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/3",
						Value:   "1",
						LeaseID: 512,
					},
				},
			},
		},
		{
			name: "add-node-when-partitions-persisted",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				LastPartition: 1,
				NodeID:        2,
				Revision:      200,
			},
			selfNodeID: 1,
			stateBefore: func(s *state) {
				s.leaseID = 666
				s.nodeMap = map[NodeID]Node{
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 120,
					},
					{},
					{},
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 130,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/0",
						ModRevision: 120,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/2",
						Value:   "1",
						LeaseID: 666,
					},
				},
				startPartitions: []PartitionID{3},
			},
		},
		{
			name: "delete-node",
			event: NodeEvent{
				Type:          EtcdEventTypeDelete,
				NodeID:        1,
				LastPartition: 3,
				Revision:      300,
			},
			selfNodeID: 1,
			stateBefore: func(s *state) {
				s.leaseID = 777
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 120,
						Running:     true,
					},
					{
						Persisted:   true,
						Owner:       2,
						ModRevision: 220,
					},
					{},
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 140,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/0",
						ModRevision: 120,
					},
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/3",
						ModRevision: 140,
					},
				},
				stopPartitions: []PartitionID{0},
			},
		},
		{
			name: "delete-only-remain-node",
			event: NodeEvent{
				Type:          EtcdEventTypeDelete,
				NodeID:        1,
				LastPartition: 3,
				Revision:      300,
			},
			stateBefore: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 120,
					},
					{
						Persisted:   true,
						Owner:       2,
						ModRevision: 220,
					},
					{},
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 120,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodeMap = map[NodeID]Node{}
			},
			output: runLoopOutput{},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan NodeEvent, 1)
			ch <- e.event

			s := newState(4, "/partition/", "/node/", e.selfNodeID, e.selfLastPartition)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := s.clone()
			e.stateAfter(stateAfter)

			output := s.runLoop(context.Background(), nil, ch, nil, nil)

			assert.Equal(t, stateAfter, s)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestRunLoop_PartitionEvent(t *testing.T) {
	table := []struct {
		name        string
		event       PartitionEvents
		stateBefore func(s *state)
		stateAfter  func(s *state)

		output runLoopOutput
	}{
		{
			name: "add-two-partition.lease-exist",
			event: PartitionEvents{
				Events: []PartitionEvent{
					{
						Type:      EtcdEventTypePut,
						Partition: 0,
						Owner:     1,
					},
					{
						Type:      EtcdEventTypePut,
						Partition: 2,
						Owner:     2,
					},
				},
				Revision: 200,
			},
			stateBefore: func(s *state) {
				s.leaseID = 222
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   30,
					},
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   20,
					},
				}
			},
			stateAfter: func(s *state) {
				s.partitions = []Partition{
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 200,
					},
					{},
					{
						Persisted:   true,
						Owner:       2,
						ModRevision: 200,
					},
					{},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/0",
						ModRevision: 200,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/3",
						Value:   "1",
						LeaseID: 222,
					},
				},
			},
		},
		{
			name: "add-two-partition.lease-not-exist",
			event: PartitionEvents{
				Events: []PartitionEvent{
					{
						Type:      EtcdEventTypePut,
						Partition: 0,
						Owner:     1,
					},
					{
						Type:      EtcdEventTypePut,
						Partition: 2,
						Owner:     2,
					},
				},
				Revision: 200,
			},
			stateBefore: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   30,
					},
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   20,
					},
				}
			},
			stateAfter: func(s *state) {
				s.partitions = []Partition{
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 200,
					},
					{},
					{
						Persisted:   true,
						Owner:       2,
						ModRevision: 200,
					},
					{},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/0",
						ModRevision: 200,
					},
				},
			},
		},
		{
			name: "add-two-remove-one-partition",
			event: PartitionEvents{
				Events: []PartitionEvent{
					{
						Type:      EtcdEventTypeDelete,
						Partition: 0,
					},
					{
						Type:      EtcdEventTypePut,
						Partition: 2,
						Owner:     2,
					},
					{
						Type:      EtcdEventTypePut,
						Partition: 1,
						Owner:     1,
					},
				},
				Revision: 200,
			},
			stateBefore: func(s *state) {
				s.leaseID = 212
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   30,
					},
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   20,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 80,
					},
					{},
					{},
					{},
				}
			},
			stateAfter: func(s *state) {
				s.partitions = []Partition{
					{},
					{
						Persisted:   true,
						Owner:       1,
						ModRevision: 200,
					},
					{
						Persisted:   true,
						Owner:       2,
						ModRevision: 200,
					},
					{},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/1",
						ModRevision: 200,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/3",
						Value:   "1",
						LeaseID: 212,
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan PartitionEvents, 1)
			ch <- e.event

			s := newState(4, "/partition/", "/node/", 1, 3)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := s.clone()
			e.stateAfter(stateAfter)

			output := s.runLoop(context.Background(), nil, nil, ch, nil)

			assert.Equal(t, stateAfter, s)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestRunLoop_Retry_After(t *testing.T) {
	table := []struct {
		name        string
		stateBefore func(s *state)
		stateAfter  func(s *state)

		output runLoopOutput
	}{
		{
			name: "no-node",
			stateBefore: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					2: {
						ID:            2,
						LastPartition: 1,
						ModRevision:   101,
					},
				}
				s.leaseID = 5566
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypePut,
						Key:         "/node/1",
						Value:       "3",
						LeaseID:     5566,
						ModRevision: 0,
					},
				},
			},
		},
		{
			name: "have-node-do-put-partitions",
			stateBefore: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					1: {
						ID:            1,
						LastPartition: 3,
						ModRevision:   101,
					},
				}
				s.leaseID = 5566
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/0",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/1",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/2",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/3",
						Value:   "1",
						LeaseID: 5566,
					},
				},
			},
		},
		{
			name: "have-node-not-the-same-last-partition",
			stateBefore: func(s *state) {
				s.nodeMap = map[NodeID]Node{
					1: {
						ID:            1,
						LastPartition: 2,
						ModRevision:   101,
					},
				}
				s.leaseID = 5566
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/0",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/1",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/2",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:    EtcdEventTypePut,
						Key:     "/partition/3",
						Value:   "1",
						LeaseID: 5566,
					},
					{
						Type:        EtcdEventTypePut,
						Key:         "/node/1",
						Value:       "3",
						LeaseID:     5566,
						ModRevision: 101,
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan time.Time, 1)
			ch <- time.Now()

			s := newState(4, "/partition/", "/node/", 1, 3)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := s.clone()

			if e.stateAfter != nil {
				e.stateAfter(stateAfter)
			}

			output := s.runLoop(context.Background(), nil, nil, nil, ch)

			assert.Equal(t, stateAfter, s)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestRunLoop_Context_Cancel(t *testing.T) {
	s := newState(4, "/partition/", "/node/", 1, 3)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	output := s.runLoop(ctx, nil, nil, nil, nil)
	assert.Equal(t, runLoopOutput{}, output)
}

func TestRunLoop_Leases(t *testing.T) {
	table := []struct {
		name              string
		selfNodeID        NodeID
		selfLastPartition PartitionID
		event             LeaseID
		stateBefore       func(s *state)
		stateAfter        func(s *state)

		output runLoopOutput
	}{
		{
			name: "when-init",

			selfNodeID:        12,
			selfLastPartition: 2,

			event: 12233,
			stateAfter: func(s *state) {
				s.leaseID = 12233
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypePut,
						Key:         "/node/12",
						Value:       "2",
						LeaseID:     12233,
						ModRevision: 0,
					},
				},
			},
		},
		{
			name: "lease-expired",

			selfNodeID:        12,
			selfLastPartition: 3,

			event: 12233,
			stateBefore: func(s *state) {
				s.leaseID = 11122

				s.nodeMap = map[NodeID]Node{
					12: {
						ID:            12,
						LastPartition: 3,
						ModRevision:   331,
					},
				}
			},
			stateAfter: func(s *state) {
				s.leaseID = 12233
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypePut,
						Key:         "/node/12",
						Value:       "3",
						LeaseID:     12233,
						ModRevision: 331,
					},
				},
			},
		},
		{
			name: "same-lease",

			selfNodeID:        12,
			selfLastPartition: 3,

			event: 12233,
			stateBefore: func(s *state) {
				s.leaseID = 12233

				s.nodeMap = map[NodeID]Node{
					12: {
						ID:            12,
						LastPartition: 3,
						ModRevision:   331,
					},
				}
			},
			stateAfter: func(s *state) {
				s.leaseID = 12233
			},
			output: runLoopOutput{},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan LeaseID, 1)
			ch <- e.event

			s := newState(4, "/partition/", "/node/", e.selfNodeID, e.selfLastPartition)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := s.clone()
			e.stateAfter(stateAfter)

			output := s.runLoop(context.Background(), ch, nil, nil, nil)

			assert.Equal(t, stateAfter, s)
			assert.Equal(t, e.output, output)
		})
	}
}

func TestPartitionActionsToOutput(t *testing.T) {
	table := []struct {
		name       string
		preOutput  runLoopOutput
		id         PartitionID
		selfNodeID NodeID
		leaseID    LeaseID
		isLeader   bool
		partition  Partition

		expected runLoopOutput
	}{
		{
			name:      "not-leader.not-persisted.not-running",
			partition: Partition{},
		},
		{
			name:       "leader.not-persisted.not-running",
			isLeader:   true,
			leaseID:    55,
			id:         11,
			selfNodeID: 33,
			partition: Partition{
				ModRevision: 123,
			},
			expected: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypePut,
						Key:         "/partition/11",
						Value:       "33",
						LeaseID:     55,
						ModRevision: 123,
					},
				},
			},
		},
		{
			name:       "leader.not-persisted.not-running.without-lease",
			isLeader:   true,
			id:         11,
			selfNodeID: 33,
			partition: Partition{
				ModRevision: 123,
			},
			expected: runLoopOutput{},
		},
		{
			name:       "not-leader.persisted-same.running",
			isLeader:   false,
			id:         9,
			selfNodeID: 33,
			partition: Partition{
				Persisted:   true,
				Owner:       33,
				ModRevision: 123,
				Running:     true,
			},
			expected: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Type:        EtcdEventTypeDelete,
						Key:         "/partition/9",
						ModRevision: 123,
					},
				},
				stopPartitions: []PartitionID{9},
			},
		},
		{
			name:       "leader.persisted-same.not-running",
			isLeader:   true,
			id:         9,
			selfNodeID: 33,
			partition: Partition{
				Persisted:   true,
				Owner:       33,
				ModRevision: 123,
				Running:     false,
			},
			expected: runLoopOutput{
				startPartitions: []PartitionID{9},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			s := newState(12, "/partition/", "/node/", e.selfNodeID, 1)
			s.leaseID = e.leaseID
			s.partitions[e.id] = e.partition

			output := s.partitionActionsToOutput(e.preOutput, e.id, e.isLeader)
			assert.Equal(t, e.expected, output)
		})
	}
}

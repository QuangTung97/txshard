package txshard

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestState_RunLoop_NodeEvent(t *testing.T) {
	table := []struct {
		name        string
		event       NodeEvent
		stateBefore func(s *state)
		stateAfter  func(s *state)

		output runLoopOutput
	}{
		{
			name: "add-node",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				NodeID:        1,
				LastPartition: 3,
				Revision:      100,
			},
			stateAfter: func(s *state) {
				s.nodes = []Node{
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Key:   "/partition/0",
						Value: "1",
					},
					{
						Key:   "/partition/1",
						Value: "1",
					},
					{
						Key:   "/partition/2",
						Value: "1",
					},
					{
						Key:   "/partition/3",
						Value: "1",
					},
				},
			},
		},
		{
			name: "add-node-not-leader",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				NodeID:        1,
				LastPartition: 3,
				Revision:      100,
			},
			stateBefore: func(s *state) {
				s.selfNodeID = 1
				s.leaderNodeID = 2
			},
			stateAfter: func(s *state) {
				s.nodes = []Node{
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{},
		},
		{
			name: "add-two-nodes",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				LastPartition: 1,
				NodeID:        2,
				Revision:      200,
			},
			stateBefore: func(s *state) {
				s.nodes = []Node{
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Key:   "/partition/0",
						Value: "2",
					},
					{
						Key:   "/partition/1",
						Value: "2",
					},
					{
						Key:   "/partition/2",
						Value: "1",
					},
					{
						Key:   "/partition/3",
						Value: "1",
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
			stateBefore: func(s *state) {
				s.nodes = []Node{
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Leader:      1,
						ModRevision: 120,
					},
					{},
					{},
					{
						Persisted:   true,
						Leader:      1,
						ModRevision: 120,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Key:         "/partition/0",
						Value:       "2",
						ModRevision: 120,
					},
					{
						Key:   "/partition/1",
						Value: "2",
					},
					{
						Key:   "/partition/2",
						Value: "1",
					},
				},
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
			stateBefore: func(s *state) {
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Leader:      1,
						ModRevision: 120,
					},
					{
						Persisted:   true,
						Leader:      2,
						ModRevision: 220,
					},
					{},
					{
						Persisted:   true,
						Leader:      1,
						ModRevision: 120,
					},
				}
			},
			stateAfter: func(s *state) {
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Key:         "/partition/0",
						Value:       "2",
						ModRevision: 120,
					},
					{
						Key:   "/partition/2",
						Value: "2",
					},
					{
						Key:         "/partition/3",
						Value:       "2",
						ModRevision: 120,
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan NodeEvent, 1)
			ch <- e.event

			s := newState(4, "/partition/", 0)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := &state{}
			*stateAfter = *s
			e.stateAfter(stateAfter)

			output := s.runLoop(context.Background(), ch, nil, nil, nil)

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
			name: "add-two-partition",
			event: PartitionEvents{
				Events: []PartitionEvent{
					{
						Type:      EtcdEventTypePut,
						Partition: 0,
						Leader:    1,
					},
					{
						Type:      EtcdEventTypePut,
						Partition: 2,
						Leader:    2,
					},
				},
				Revision: 200,
			},
			stateBefore: func(s *state) {
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   30,
					},
					{
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
						Leader:      1,
						ModRevision: 200,
					},
					{},
					{
						Persisted:   true,
						Leader:      2,
						ModRevision: 200,
					},
					{},
				}
			},
			output: runLoopOutput{
				startPartitions: []PartitionID{0},
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
						Leader:    2,
					},
					{
						Type:      EtcdEventTypePut,
						Partition: 1,
						Leader:    1,
					},
				},
				Revision: 200,
			},
			stateBefore: func(s *state) {
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   30,
					},
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   20,
					},
				}
				s.partitions = []Partition{
					{
						Persisted:   true,
						Leader:      1,
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
						Leader:      1,
						ModRevision: 200,
					},
					{
						Persisted:   true,
						Leader:      2,
						ModRevision: 200,
					},
					{},
				}
			},
			output: runLoopOutput{
				startPartitions: []PartitionID{1},
				stopPartitions:  []PartitionID{0},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan PartitionEvents, 1)
			ch <- e.event

			s := newState(4, "/partition/", 1)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := &state{}
			*stateAfter = *s
			e.stateAfter(stateAfter)

			output := s.runLoop(context.Background(), nil, ch, nil, nil)

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
			name: "not-leader",
			stateBefore: func(s *state) {
				s.leaderNodeID = 2
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
		},
		{
			name: "leader-without-nodes",
			stateBefore: func(s *state) {
				s.leaderNodeID = 1
			},
		},
		{
			name: "leader-with-nodes",
			stateBefore: func(s *state) {
				s.leaderNodeID = 1
				s.nodes = []Node{
					{
						ID:            2,
						LastPartition: 1,
						ModRevision:   200,
					},
					{
						ID:            1,
						LastPartition: 3,
						ModRevision:   100,
					},
				}
			},
			output: runLoopOutput{
				kvs: []CASKeyValue{
					{
						Key:   "/partition/0",
						Value: "2",
					},
					{
						Key:   "/partition/1",
						Value: "2",
					},
					{
						Key:   "/partition/2",
						Value: "1",
					},
					{
						Key:   "/partition/3",
						Value: "1",
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan time.Time, 1)
			ch <- time.Now()

			s := newState(4, "/partition/", 1)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := &state{}
			*stateAfter = *s

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
	s := newState(4, "/partition/", 1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	output := s.runLoop(ctx, nil, nil, nil, nil)
	assert.Equal(t, runLoopOutput{}, output)
}

func TestRunLoop_LeaderEvent(t *testing.T) {
	table := []struct {
		name        string
		event       LeaderEvent
		stateBefore func(s *state)
		stateAfter  func(s *state)

		output runLoopOutput
	}{
		{
			name: "put-leader",
			event: LeaderEvent{
				Type:   EtcdEventTypePut,
				Leader: 2,
			},
			stateBefore: func(s *state) {
				s.selfNodeID = 1
				s.leaderNodeID = 0
			},
			stateAfter: func(s *state) {
				s.leaderNodeID = 2
			},
		},
		{
			name: "delete-leader",
			event: LeaderEvent{
				Type:   EtcdEventTypeDelete,
			},
			stateBefore: func(s *state) {
				s.selfNodeID = 1
				s.leaderNodeID = 2
			},
			stateAfter: func(s *state) {
				s.leaderNodeID = 0
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ch := make(chan LeaderEvent, 1)
			ch <- e.event

			s := newState(4, "/partition/", 1)

			if e.stateBefore != nil {
				e.stateBefore(s)
			}

			stateAfter := &state{}
			*stateAfter = *s
			e.stateAfter(stateAfter)

			output := s.runLoop(context.Background(), nil, nil, ch, nil)

			assert.Equal(t, stateAfter, s)
			assert.Equal(t, e.output, output)
		})
	}
}

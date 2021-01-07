package txshard

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestState_RunLoop_NodeEvent(t *testing.T) {
	table := []struct {
		name        string
		event       NodeEvent
		stateBefore func(s *state)
		stateAfter  func(s *state)

		output runLoopOutput
		err    error
	}{
		{
			name: "add-node",
			event: NodeEvent{
				Type:          EtcdEventTypePut,
				Node:          1,
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
				Node:          1,
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
				Node:          2,
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
				Node:          2,
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
						NodeID:      1,
						ModRevision: 120,
					},
					{},
					{},
					{
						Persisted:   true,
						NodeID:      1,
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
				Node:          1,
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
						NodeID:      1,
						ModRevision: 120,
					},
					{
						Persisted:   true,
						NodeID:      2,
						ModRevision: 220,
					},
					{},
					{
						Persisted:   true,
						NodeID:      1,
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

			output, err := s.runLoop(context.Background(), ch, nil, nil)

			assert.Equal(t, e.err, err)
			assert.Equal(t, stateAfter, s)
			assert.Equal(t, e.output, output)
		})
	}
}

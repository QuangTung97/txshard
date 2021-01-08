package txshard

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Node ...
type Node struct {
	ID            NodeID
	LastPartition PartitionID
	ModRevision   Revision
}

// Partition ...
type Partition struct {
	Persisted   bool
	Leader      NodeID
	ModRevision Revision
}

type state struct {
	partitionPrefix string
	partitionCount  PartitionID

	leaderNodeID NodeID
	selfNodeID   NodeID

	nodes      []Node
	partitions []Partition
}

func newState(partitionCount PartitionID, partitionPrefix string, selfNodeID NodeID) *state {
	return &state{
		partitionPrefix: partitionPrefix,
		partitionCount:  partitionCount,

		selfNodeID: selfNodeID,
		partitions: make([]Partition, partitionCount),
	}
}

type runLoopOutput struct {
	kvs             []CASKeyValue
	startPartitions []PartitionID
	stopPartitions  []PartitionID
}

type sortNode []Node

var _ sort.Interface = sortNode{}

func (s sortNode) Len() int {
	return len(s)
}
func (s sortNode) Less(i, j int) bool {
	return s[i].LastPartition < s[j].LastPartition
}

func (s sortNode) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func computeNodeIDFromPartitionID(nodes []Node, partition PartitionID) NodeID {
	for _, n := range nodes {
		if n.LastPartition >= partition {
			return n.ID
		}
	}
	return nodes[0].ID
}

func (s *state) computePartitionKvs() []CASKeyValue {
	kvs := make([]CASKeyValue, 0)
	for partitionID := PartitionID(0); partitionID < s.partitionCount; partitionID++ {
		nodeID := computeNodeIDFromPartitionID(s.nodes, partitionID)
		partition := s.partitions[partitionID]

		if partition.Persisted && partition.Leader == nodeID {
			continue
		}

		revision := Revision(0)
		if partition.Persisted {
			revision = partition.ModRevision
		}

		kvs = append(kvs, CASKeyValue{
			Key:         s.partitionPrefix + fmt.Sprintf("%d", partitionID),
			Value:       fmt.Sprintf("%d", nodeID),
			ModRevision: revision,
		})
	}
	return kvs
}

func deleteNodesByID(nodes []Node, nodeID NodeID) []Node {
	result := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.ID == nodeID {
			continue
		}
		result = append(result, n)
	}
	return result
}

func (s *state) runLoopHandleNodeEvent(nodeEvent NodeEvent) runLoopOutput {
	if nodeEvent.Type == EtcdEventTypePut {
		s.nodes = append(s.nodes, Node{
			ID:            nodeEvent.NodeID,
			LastPartition: nodeEvent.LastPartition,
			ModRevision:   nodeEvent.Revision,
		})
	} else if nodeEvent.Type == EtcdEventTypeDelete {
		s.nodes = deleteNodesByID(s.nodes, nodeEvent.NodeID)
	}

	sort.Sort(sortNode(s.nodes))

	var kvs []CASKeyValue
	if len(s.nodes) > 0 && s.leaderNodeID == s.selfNodeID {
		kvs = s.computePartitionKvs()
	}

	return runLoopOutput{
		kvs: kvs,
	}
}

func (s *state) runLoopHandlePartitionEvent(events PartitionEvents) runLoopOutput {
	var startPartitions []PartitionID
	var stopPartitions []PartitionID

	for _, e := range events.Events {
		oldPartition := s.partitions[e.Partition]
		oldIsRunning := oldPartition.Persisted && oldPartition.Leader == s.selfNodeID

		var newPartition Partition
		if e.Type == EtcdEventTypePut {
			newPartition = Partition{
				Persisted:   true,
				Leader:      e.Leader,
				ModRevision: events.Revision,
			}
		} else {
			newPartition = Partition{
				Persisted:   false,
				Leader:      0,
				ModRevision: 0,
			}
		}

		s.partitions[e.Partition] = newPartition

		newIsRunning := newPartition.Persisted && newPartition.Leader == s.selfNodeID
		if newIsRunning != oldIsRunning {
			if newIsRunning {
				startPartitions = append(startPartitions, e.Partition)
			} else {
				stopPartitions = append(stopPartitions, e.Partition)
			}
		}
	}

	return runLoopOutput{
		startPartitions: startPartitions,
		stopPartitions:  stopPartitions,
	}
}

func (s *state) runLoop(
	ctx context.Context,
	nodeEvents <-chan NodeEvent,
	partitionEventsChan <-chan PartitionEvents,
	leaderEvents <-chan LeaderEvent,
	after <-chan time.Time,
) runLoopOutput {
	select {
	case nodeEvent := <-nodeEvents:
		return s.runLoopHandleNodeEvent(nodeEvent)

	case partitionEvent := <-partitionEventsChan:
		return s.runLoopHandlePartitionEvent(partitionEvent)

	case leaderEvent := <-leaderEvents:
		if leaderEvent.Type == EtcdEventTypePut {
			s.leaderNodeID = leaderEvent.Leader
		} else {
			s.leaderNodeID = 0
		}
		return runLoopOutput{}

	case <-after:
		var kvs []CASKeyValue
		if len(s.nodes) > 0 && s.leaderNodeID == s.selfNodeID {
			kvs = s.computePartitionKvs()
		}
		return runLoopOutput{
			kvs: kvs,
		}

	case <-ctx.Done():
		return runLoopOutput{}
	}
}

package txshard

import (
	"context"
	"fmt"
	"sort"
	"sync"
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
	Owner       NodeID
	ModRevision Revision
}

type state struct {
	partitionPrefix string
	partitionCount  PartitionID
	nodePrefix      string

	selfNodeID        NodeID
	selfLastPartition PartitionID

	leaseID      LeaseID
	leaderNodeID NodeID

	nodeMapMut sync.RWMutex
	nodeMap    map[NodeID]Node

	partitionsMut sync.RWMutex
	partitions    []Partition
}

func newState(
	partitionCount PartitionID, partitionPrefix string,
	nodePrefix string, selfNodeID NodeID, selfLastPartition PartitionID,
) *state {
	return &state{
		partitionPrefix: partitionPrefix,
		partitionCount:  partitionCount,
		nodePrefix:      nodePrefix,

		selfNodeID:        selfNodeID,
		selfLastPartition: selfLastPartition,

		partitions: make([]Partition, partitionCount),
	}
}

func (s *state) clone() *state {
	return &state{
		partitionPrefix: s.partitionPrefix,
		partitionCount:  s.partitionCount,
		nodePrefix:      s.nodePrefix,

		selfNodeID:        s.selfNodeID,
		selfLastPartition: s.selfLastPartition,

		leaseID:      s.leaseID,
		leaderNodeID: s.leaderNodeID,

		nodeMap:    s.nodeMap,
		partitions: s.partitions,
	}
}

func (s *state) getNodeForPartition(partition PartitionID) Node {
	s.nodeMapMut.RLock()
	s.partitionsMut.RLock()

	nodeID := s.partitions[partition].Owner
	m := s.nodeMap

	s.partitionsMut.RUnlock()
	s.nodeMapMut.RUnlock()

	return m[nodeID]
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
	s.nodeMapMut.RLock()
	nodes := make([]Node, 0, len(s.nodeMap))
	for _, n := range s.nodeMap {
		nodes = append(nodes, n)
	}
	s.nodeMapMut.RUnlock()

	sort.Sort(sortNode(nodes))

	kvs := make([]CASKeyValue, 0)

	s.partitionsMut.RLock()
	for partitionID := PartitionID(0); partitionID < s.partitionCount; partitionID++ {
		nodeID := computeNodeIDFromPartitionID(nodes, partitionID)
		partition := s.partitions[partitionID]

		if partition.Persisted && partition.Owner == nodeID {
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
	s.partitionsMut.RUnlock()

	return kvs
}

func deleteNodeByID(nodeMap map[NodeID]Node, nodeID NodeID) map[NodeID]Node {
	result := make(map[NodeID]Node)

	for id, n := range nodeMap {
		if id == nodeID {
			continue
		}
		result[id] = n
	}

	return result
}

func addNode(nodeMap map[NodeID]Node, n Node) map[NodeID]Node {
	result := make(map[NodeID]Node)

	for id, n := range nodeMap {
		result[id] = n
	}
	result[n.ID] = n

	return result
}

func (s *state) runLoopHandleNodeEvent(nodeEvent NodeEvent) runLoopOutput {
	if nodeEvent.Type == EtcdEventTypePut {
		s.nodeMapMut.Lock()
		s.nodeMap = addNode(s.nodeMap, Node{
			ID:            nodeEvent.NodeID,
			LastPartition: nodeEvent.LastPartition,
			ModRevision:   nodeEvent.Revision,
		})
		s.nodeMapMut.Unlock()
	} else if nodeEvent.Type == EtcdEventTypeDelete {
		s.nodeMapMut.Lock()
		s.nodeMap = deleteNodeByID(s.nodeMap, nodeEvent.NodeID)
		s.nodeMapMut.Unlock()
	}

	var kvs []CASKeyValue

	s.nodeMapMut.RLock()
	nodeMapLen := len(s.nodeMap)
	s.nodeMapMut.RUnlock()

	if nodeMapLen > 0 && s.leaderNodeID == s.selfNodeID {
		kvs = s.computePartitionKvs()
	}

	return runLoopOutput{
		kvs: kvs,
	}
}

func (s *state) runLoopHandlePartitionEvent(events PartitionEvents) runLoopOutput {
	var startPartitions []PartitionID
	var stopPartitions []PartitionID

	s.partitionsMut.Lock()
	for _, e := range events.Events {
		oldPartition := s.partitions[e.Partition]
		oldIsRunning := oldPartition.Persisted && oldPartition.Owner == s.selfNodeID

		var newPartition Partition
		if e.Type == EtcdEventTypePut {
			newPartition = Partition{
				Persisted:   true,
				Owner:       e.Leader,
				ModRevision: events.Revision,
			}
		} else {
			newPartition = Partition{
				Persisted:   false,
				Owner:       0,
				ModRevision: 0,
			}
		}

		s.partitions[e.Partition] = newPartition

		newIsRunning := newPartition.Persisted && newPartition.Owner == s.selfNodeID
		if newIsRunning != oldIsRunning {
			if newIsRunning {
				startPartitions = append(startPartitions, e.Partition)
			} else {
				stopPartitions = append(stopPartitions, e.Partition)
			}
		}
	}
	s.partitionsMut.Unlock()

	return runLoopOutput{
		startPartitions: startPartitions,
		stopPartitions:  stopPartitions,
	}
}

func (s *state) runLoop(
	ctx context.Context,
	leaseEvents <-chan LeaseID,
	nodeEvents <-chan NodeEvent,
	partitionEventsChan <-chan PartitionEvents,
	leaderEvents <-chan LeaderEvent,
	after <-chan time.Time,
) runLoopOutput {
	select {
	case leaseID := <-leaseEvents:
		oldLeaseID := s.leaseID

		var kvs []CASKeyValue
		if leaseID != oldLeaseID {
			modRevision := Revision(0)

			s.nodeMapMut.RLock()
			for id, n := range s.nodeMap {
				if id == s.selfNodeID {
					modRevision = n.ModRevision
				}
			}
			s.nodeMapMut.RUnlock()

			kvs = append(kvs, CASKeyValue{
				Key:         s.nodePrefix + fmt.Sprintf("%d", s.selfNodeID),
				Value:       fmt.Sprintf("%d", s.selfLastPartition),
				LeaseID:     leaseID,
				ModRevision: modRevision,
			})
		}

		s.leaseID = leaseID
		return runLoopOutput{
			kvs: kvs,
		}

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

		s.nodeMapMut.RLock()
		nodeMapLen := len(s.nodeMap)
		s.nodeMapMut.RUnlock()

		if nodeMapLen > 0 && s.leaderNodeID == s.selfNodeID {
			kvs = s.computePartitionKvs()
		}

		if s.leaseID != 0 {
			found := false
			var node Node

			s.nodeMapMut.RLock()
			for id, n := range s.nodeMap {
				if id == s.selfNodeID {
					found = true
					node = n
					break
				}
			}
			s.nodeMapMut.RUnlock()

			if !found || node.LastPartition != s.selfLastPartition {
				kvs = append(kvs, CASKeyValue{
					Key:         s.nodePrefix + fmt.Sprintf("%d", s.selfNodeID),
					Value:       fmt.Sprintf("%d", s.selfLastPartition),
					LeaseID:     s.leaseID,
					ModRevision: 0,
				})
			}
		}

		return runLoopOutput{
			kvs: kvs,
		}

	case <-ctx.Done():
		return runLoopOutput{}
	}
}

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
	Running     bool
	ModRevision Revision
}

type state struct {
	partitionPrefix string
	partitionCount  PartitionID
	nodePrefix      string

	selfNodeID        NodeID
	selfLastPartition PartitionID

	leaseID LeaseID

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

		nodeMap:    make(map[NodeID]Node),
		partitions: make([]Partition, partitionCount),
	}
}

func cloneNodeMap(nodeMap map[NodeID]Node) map[NodeID]Node {
	result := make(map[NodeID]Node)
	for k, v := range nodeMap {
		result[k] = v
	}
	return result
}

func clonePartitions(partitions []Partition) []Partition {
	result := make([]Partition, len(partitions))
	copy(result, partitions)
	return result
}

func (s *state) clone() *state {
	return &state{
		partitionPrefix: s.partitionPrefix,
		partitionCount:  s.partitionCount,
		nodePrefix:      s.nodePrefix,

		selfNodeID:        s.selfNodeID,
		selfLastPartition: s.selfLastPartition,

		leaseID: s.leaseID,

		nodeMap:    cloneNodeMap(s.nodeMap),
		partitions: clonePartitions(s.partitions),
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

func (s *state) computePartitionsActions() runLoopOutput {
	s.nodeMapMut.RLock()
	nodeMapLen := len(s.nodeMap)
	s.nodeMapMut.RUnlock()

	if nodeMapLen == 0 {
		return runLoopOutput{}
	}

	s.nodeMapMut.RLock()
	nodes := make([]Node, 0, len(s.nodeMap))
	for _, n := range s.nodeMap {
		nodes = append(nodes, n)
	}
	s.nodeMapMut.RUnlock()

	sort.Sort(sortNode(nodes))

	output := runLoopOutput{}

	s.partitionsMut.RLock()
	for partitionID := PartitionID(0); partitionID < s.partitionCount; partitionID++ {
		nodeID := computeNodeIDFromPartitionID(nodes, partitionID)

		output = s.partitionActionsToOutput(output, partitionID, nodeID == s.selfNodeID)
	}
	s.partitionsMut.RUnlock()

	return output
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
	s.nodeMapMut.Lock()
	if nodeEvent.Type == EtcdEventTypePut {
		s.nodeMap = addNode(s.nodeMap, Node{
			ID:            nodeEvent.NodeID,
			LastPartition: nodeEvent.LastPartition,
			ModRevision:   nodeEvent.Revision,
		})
	} else if nodeEvent.Type == EtcdEventTypeDelete {
		s.nodeMap = deleteNodeByID(s.nodeMap, nodeEvent.NodeID)
	}
	s.nodeMapMut.Unlock()

	return s.computePartitionsActions()
}

func (s *state) runLoopHandlePartitionEvent(events PartitionEvents) runLoopOutput {
	s.partitionsMut.Lock()
	for _, e := range events.Events {
		var newPartition Partition
		if e.Type == EtcdEventTypePut {
			newPartition = Partition{
				Persisted:   true,
				Owner:       e.Owner,
				ModRevision: e.Revision,
			}
		} else {
			newPartition = Partition{
				Persisted:   false,
				Owner:       0,
				ModRevision: 0,
			}
		}

		s.partitions[e.Partition] = newPartition
	}
	s.partitionsMut.Unlock()

	return s.computePartitionsActions()
}

func (s *state) partitionActionsToOutput(output runLoopOutput, id PartitionID, isLeader bool) runLoopOutput {
	persisted := s.partitions[id].Persisted
	isSame := s.partitions[id].Owner == s.selfNodeID
	running := s.partitions[id].Running

	actions := computePartitionActions(isLeader, persisted, isSame, running)
	if s.leaseID != 0 {
		if actions.put {
			output.kvs = append(output.kvs, CASKeyValue{
				Type:        EtcdEventTypePut,
				Key:         s.partitionPrefix + fmt.Sprintf("%d", id),
				Value:       fmt.Sprintf("%d", s.selfNodeID),
				LeaseID:     s.leaseID,
				ModRevision: s.partitions[id].ModRevision,
			})
		}
	}

	if actions.delete {
		output.kvs = append(output.kvs, CASKeyValue{
			Type:        EtcdEventTypeDelete,
			Key:         s.partitionPrefix + fmt.Sprintf("%d", id),
			ModRevision: s.partitions[id].ModRevision,
		})
	}

	if actions.start {
		output.startPartitions = append(output.startPartitions, id)
	}

	if actions.stop {
		output.stopPartitions = append(output.stopPartitions, id)
	}

	return output
}

func (s *state) computeSelfNodeActions(kvs []CASKeyValue, needUpdate bool) []CASKeyValue {
	if s.leaseID == 0 {
		return kvs
	}

	modRevision := Revision(0)

	s.nodeMapMut.RLock()
	node, ok := s.nodeMap[s.selfNodeID]
	s.nodeMapMut.RUnlock()

	if ok {
		modRevision = node.ModRevision
	}

	if !ok || node.LastPartition != s.selfLastPartition || needUpdate {
		kvs = append(kvs, CASKeyValue{
			Type:        EtcdEventTypePut,
			Key:         s.nodePrefix + fmt.Sprintf("%d", s.selfNodeID),
			Value:       fmt.Sprintf("%d", s.selfLastPartition),
			LeaseID:     s.leaseID,
			ModRevision: modRevision,
		})
	}

	return kvs
}

func (s *state) runLoop(
	ctx context.Context,
	leaseEvents <-chan LeaseID,
	nodeEvents <-chan NodeEvent,
	partitionEventsChan <-chan PartitionEvents,
	runnerChan <-chan RunnerEvents,
	after <-chan time.Time,
) runLoopOutput {
	select {
	case leaseID := <-leaseEvents:
		oldLeaseID := s.leaseID
		s.leaseID = leaseID

		var kvs []CASKeyValue
		if leaseID != oldLeaseID {
			kvs = s.computeSelfNodeActions(kvs, true)
		}

		return runLoopOutput{
			kvs: kvs,
		}

	case nodeEvent := <-nodeEvents:
		output := s.runLoopHandleNodeEvent(nodeEvent)
		output.kvs = s.computeSelfNodeActions(output.kvs, false)
		return output

	case partitionEvent := <-partitionEventsChan:
		return s.runLoopHandlePartitionEvent(partitionEvent)

	case runnerEvents := <-runnerChan:
		s.partitionsMut.Lock()
		for _, event := range runnerEvents.Events {
			if event.Type == RunnerEventTypeStart {
				s.partitions[event.Partition].Running = true
			} else {
				s.partitions[event.Partition].Running = false
			}
		}
		s.partitionsMut.Unlock()
		return runLoopOutput{}

	case <-after:
		output := s.computePartitionsActions()
		output.kvs = s.computeSelfNodeActions(output.kvs, false)
		return output

	case <-ctx.Done():
		return runLoopOutput{}
	}
}

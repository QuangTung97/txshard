package txshard

import (
	"context"
)

// Revision ...
type Revision int64

// LeaseID ...
type LeaseID int64

// CASKeyValue ...
type CASKeyValue struct {
	Key         string
	Value       string
	LeaseID     LeaseID
	ModRevision Revision
}

// EtcdEventType ...
type EtcdEventType int

const (
	// EtcdEventTypePut ...
	EtcdEventTypePut EtcdEventType = 1
	// EtcdEventTypeDelete ...
	EtcdEventTypeDelete EtcdEventType = 2
)

// PartitionID ...
type PartitionID uint32

// NodeID ...
type NodeID uint32

// PartitionEvent ...
type PartitionEvent struct {
	Type      EtcdEventType
	Partition PartitionID
	Leader    NodeID
}

// PartitionEvents ...
type PartitionEvents struct {
	Events   []PartitionEvent
	Revision Revision
}

// NodeEvent ...
type NodeEvent struct {
	Type          EtcdEventType
	NodeID        NodeID
	LastPartition PartitionID
	LeaseID       LeaseID
	Revision      Revision
}

// LeaderEvent ...
type LeaderEvent struct {
	Type   EtcdEventType
	Leader NodeID
}

// EtcdClient ...
type EtcdClient interface {
	CompareAndSet(ctx context.Context, kvs []CASKeyValue) error
}

// Processor ...
type Processor struct {
}

// Run ...
func (p *Processor) Run(ctx context.Context) {
}

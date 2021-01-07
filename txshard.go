package txshard

import (
	"context"
)

// Revision ...
type Revision int64

// CASKeyValue ...
type CASKeyValue struct {
	Key         string
	Value       string
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

// LeaderID ...
type LeaderID uint32

// NodeID ...
type NodeID uint32

// PartitionEvent ...
type PartitionEvent struct {
	Type      EtcdEventType
	Partition PartitionID
	Leader    LeaderID
	Revision  Revision
}

// NodeEvent ...
type NodeEvent struct {
	Type          EtcdEventType
	Node          NodeID
	LastPartition PartitionID
	Revision      Revision
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

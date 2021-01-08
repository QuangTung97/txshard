package txshard

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"time"
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

// Runner ...
type Runner func(ctx context.Context, partitionID PartitionID)

type activeRunner struct {
	cancel context.CancelFunc
}

// Processor ...
type Processor struct {
	state  *state
	client EtcdClient
	runner Runner
	logger *zap.Logger

	timeoutDuration time.Duration

	leaseChan     <-chan LeaseID
	nodeChan      <-chan NodeEvent
	partitionChan <-chan PartitionEvents
	leaderChan    <-chan LeaderEvent

	activeMap map[PartitionID]activeRunner
	wg        sync.WaitGroup
}

// Config ...
type Config struct {
	PartitionCount    PartitionID
	PartitionPrefix   string
	NodePrefix        string
	SelfNodeID        NodeID
	SelfLastPartition PartitionID

	Client EtcdClient
	Runner Runner
	Logger *zap.Logger

	LeaseChan     <-chan LeaseID
	NodeChan      <-chan NodeEvent
	PartitionChan <-chan PartitionEvents
	LeaderChan    <-chan LeaderEvent
}

// NewProcessor ...
func NewProcessor(conf Config) *Processor {
	st := newState(
		conf.PartitionCount, conf.PartitionPrefix, conf.NodePrefix,
		conf.SelfNodeID, conf.SelfLastPartition,
	)

	timeout := 60 * time.Second

	return &Processor{
		state:  st,
		client: conf.Client,
		runner: conf.Runner,
		logger: conf.Logger,

		timeoutDuration: timeout,

		leaseChan:     conf.LeaseChan,
		nodeChan:      conf.NodeChan,
		partitionChan: conf.PartitionChan,
		leaderChan:    conf.LeaderChan,
	}
}

// Run ...
func (p *Processor) Run(ctx context.Context) {
	var after <-chan time.Time
	for {
		output := p.state.runLoop(ctx, p.leaseChan, p.nodeChan, p.partitionChan, p.leaderChan, after)
		if ctx.Err() != nil {
			p.wg.Wait()
			return
		}

		if len(output.kvs) > 0 {
			err := p.client.CompareAndSet(ctx, output.kvs)
			if err != nil {
				p.logger.Error("client.CompareAndSet", zap.Any("kvs", output.kvs))
				after = time.After(p.timeoutDuration)
				continue
			}
		}

		for _, partition := range output.startPartitions {
			p.wg.Add(1)
			ctx, cancel := context.WithCancel(ctx)

			go func() {
				defer p.wg.Done()
				p.runner(ctx, partition)
			}()

			p.activeMap[partition] = activeRunner{
				cancel: cancel,
			}
		}

		for _, partition := range output.stopPartitions {
			p.activeMap[partition].cancel()
			delete(p.activeMap, partition)
		}
	}
}

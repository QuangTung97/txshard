package txshard

import (
	"context"
	"go.uber.org/zap"
	"time"
)

// Revision ...
type Revision int64

// LeaseID ...
type LeaseID int64

// EtcdEventType ...
type EtcdEventType int

const (
	// EtcdEventTypePut ...
	EtcdEventTypePut EtcdEventType = 1
	// EtcdEventTypeDelete ...
	EtcdEventTypeDelete EtcdEventType = 2
)

// CASKeyValue ...
type CASKeyValue struct {
	Type        EtcdEventType
	Key         string
	Value       string
	LeaseID     LeaseID
	ModRevision Revision
}

// PartitionID ...
type PartitionID uint32

// NodeID ...
type NodeID uint32

// PartitionEvent ...
type PartitionEvent struct {
	Type      EtcdEventType
	Partition PartitionID
	Owner     NodeID
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

// RunnerEventType ...
type RunnerEventType int

const (
	// RunnerEventTypeStart ...
	RunnerEventTypeStart RunnerEventType = 1
	// RunnerEventTypeStop ...
	RunnerEventTypeStop RunnerEventType = 2
)

// RunnerEvent ...
type RunnerEvent struct {
	Type      RunnerEventType
	Partition PartitionID
}

// RunnerEvents ...
type RunnerEvents struct {
	Events []RunnerEvent
}

// EtcdClient ...
type EtcdClient interface {
	CompareAndSet(ctx context.Context, kvs []CASKeyValue) error
}

// Runner ...
type Runner func(ctx context.Context, partitionID PartitionID)

type activeRunner struct {
	cancel context.CancelFunc
	done   <-chan struct{}
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

	activeMap map[PartitionID]activeRunner
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
	}
}

// Run ...
func (p *Processor) Run(ctx context.Context) {
	runnerChan := make(chan RunnerEvents, p.state.partitionCount)

	var after <-chan time.Time
	for {
		output := p.state.runLoop(ctx, p.leaseChan, p.nodeChan, p.partitionChan, runnerChan, after)
		if ctx.Err() != nil {
			for _, runner := range p.activeMap {
				runner.cancel()
				<-runner.done
			}
			p.activeMap = make(map[PartitionID]activeRunner)

			return
		}

		if len(output.kvs) > 0 {
			err := p.client.CompareAndSet(ctx, output.kvs)
			if err != nil {
				p.logger.Error("client.CompareAndSet", zap.Error(err),
					zap.Any("kvs", output.kvs),
				)
				after = time.After(p.timeoutDuration)
				continue
			}
		}

		var runnerEvents []RunnerEvent

		for _, partition := range output.startPartitions {
			ctx, cancel := context.WithCancel(ctx)

			done := make(chan struct{}, 1)

			go func() {
				p.runner(ctx, partition)
				done <- struct{}{}
			}()

			p.activeMap[partition] = activeRunner{
				cancel: cancel,
				done:   done,
			}

			runnerEvents = append(runnerEvents, RunnerEvent{
				Type:      RunnerEventTypeStart,
				Partition: partition,
			})
		}

		for _, partition := range output.stopPartitions {
			p.activeMap[partition].cancel()
			<-p.activeMap[partition].done

			delete(p.activeMap, partition)

			runnerEvents = append(runnerEvents, RunnerEvent{
				Type:      RunnerEventTypeStop,
				Partition: partition,
			})
		}

		if len(runnerEvents) > 0 {
			runnerChan <- RunnerEvents{
				Events: runnerEvents,
			}
		}
	}
}

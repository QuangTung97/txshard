package txshard

import (
	"context"
	"errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

// EtcdManager ...
type EtcdManager struct {
	client *clientv3.Client
	logger *zap.Logger

	leaseChan chan LeaseID
}

var _ EtcdClient = &EtcdManager{}

// ErrTxnFailed ...
var ErrTxnFailed = errors.New("etcd txn failed")

// NewEtcdManager ...
func NewEtcdManager(logger *zap.Logger, conf clientv3.Config) *EtcdManager {
	client, err := clientv3.New(conf)
	if err != nil {
		panic(err)
	}

	logger.Debug("EtcdManager created")

	return &EtcdManager{
		client:    client,
		logger:    logger,
		leaseChan: make(chan LeaseID, 1),
	}
}

// GetLeaseChan ...
func (m *EtcdManager) GetLeaseChan() <-chan LeaseID {
	return m.leaseChan
}

// Run ...
func (m *EtcdManager) Run(ctx context.Context) {
	for {
		sess, err := concurrency.NewSession(m.client)
		if err != nil {
			m.logger.Error("concurrency.NewSession", zap.Error(err))
			continue
		}

		m.leaseChan <- LeaseID(sess.Lease())

		select {
		case <-sess.Done():
			m.logger.Error("lease expired")
			continue
		case <-ctx.Done():
			_ = sess.Close()
			return
		}
	}
}

// CompareAndSet ...
func (m *EtcdManager) CompareAndSet(ctx context.Context, kvs []CASKeyValue) error {
	m.logger.Debug("CompareAndSet", zap.Any("etcd.kvs", kvs))

	var compares []clientv3.Cmp
	var ops []clientv3.Op
	for _, kv := range kvs {
		if kv.ModRevision != 0 {
			compares = append(compares,
				clientv3.Compare(clientv3.ModRevision(kv.Key), "=", int64(kv.ModRevision)),
			)
		}

		if kv.Type == EtcdEventTypePut {
			op := clientv3.OpPut(kv.Key, kv.Value,
				clientv3.WithLease(clientv3.LeaseID(kv.LeaseID)))
			ops = append(ops, op)
		} else {
			op := clientv3.OpDelete(kv.Key)
			ops = append(ops, op)
		}
	}

	res, err := m.client.Txn(ctx).If(compares...).Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return ErrTxnFailed
	}

	return nil
}

func etcdEventTypeFromLib(eventType mvccpb.Event_EventType) EtcdEventType {
	switch eventType {
	case mvccpb.PUT:
		return EtcdEventTypePut
	case mvccpb.DELETE:
		return EtcdEventTypeDelete
	default:
		panic("unrecognized event type")
	}
}

func getNumberWithPrefix(s string, prefix string) int64 {
	num, err := strconv.ParseInt(strings.TrimPrefix(s, prefix), 10, 64)
	if err != nil {
		panic(err)
	}
	return num
}

func getNumber(s string) int64 {
	num, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return num
}

// WatchNodes ...
func (m *EtcdManager) WatchNodes(ctx context.Context, prefix string) <-chan NodeEvent {
	res, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	rev := res.Header.Revision

	result := make(chan NodeEvent, len(res.Kvs))
	for _, kv := range res.Kvs {
		event := NodeEvent{
			Type:          EtcdEventTypePut,
			NodeID:        NodeID(getNumberWithPrefix(string(kv.Key), prefix)),
			LastPartition: PartitionID(getNumber(string(kv.Value))),
			Revision:      Revision(kv.ModRevision),
		}
		m.logger.Debug("WatchNodes", zap.Any("node.event", event))
		result <- event
	}

	ch := m.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))

	go func() {
		for wr := range ch {
			revision := wr.Header.Revision
			for _, e := range wr.Events {
				eventType := etcdEventTypeFromLib(e.Type)
				lastPartition := PartitionID(0)
				if eventType == EtcdEventTypePut {
					lastPartition = PartitionID(getNumber(string(e.Kv.Value)))
				}

				event := NodeEvent{
					Type:          eventType,
					NodeID:        NodeID(getNumberWithPrefix(string(e.Kv.Key), prefix)),
					LastPartition: lastPartition,
					Revision:      Revision(revision),
				}
				m.logger.Debug("WatchNodes", zap.Any("node.event", event))
				result <- event
			}
		}
	}()

	return result
}

// WatchPartitions ...
func (m *EtcdManager) WatchPartitions(ctx context.Context, prefix string) <-chan PartitionEvents {
	res, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	rev := res.Header.Revision

	var events []PartitionEvent

	for _, kv := range res.Kvs {
		events = append(events, PartitionEvent{
			Type:      EtcdEventTypePut,
			Partition: PartitionID(getNumberWithPrefix(string(kv.Key), prefix)),
			Owner:     NodeID(getNumber(string(kv.Value))),
			Revision:  Revision(kv.ModRevision),
		})
	}

	result := make(chan PartitionEvents, 1)
	if len(events) > 0 {
		m.logger.Debug("WatchPartitions", zap.Any("partition.events", events))
		result <- PartitionEvents{
			Events: events,
		}
	}

	ch := m.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))

	go func() {
		for wr := range ch {
			var events []PartitionEvent
			for _, e := range wr.Events {
				evenType := etcdEventTypeFromLib(e.Type)
				owner := NodeID(0)
				if evenType == EtcdEventTypePut {
					owner = NodeID(getNumber(string(e.Kv.Value)))
				}

				events = append(events, PartitionEvent{
					Type:      evenType,
					Partition: PartitionID(getNumberWithPrefix(string(e.Kv.Key), prefix)),
					Owner:     owner,
					Revision:  Revision(e.Kv.ModRevision),
				})
			}
			m.logger.Debug("WatchPartitions", zap.Any("partition.events", events))
			result <- PartitionEvents{
				Events: events,
			}
		}
	}()

	return result
}

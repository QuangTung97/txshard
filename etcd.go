package txshard

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"strconv"
)

// EtcdManager ...
type EtcdManager struct {
	client *clientv3.Client
	logger *zap.Logger

	leaderChan chan<- LeaderEvent
}

// NewEtcdManager ...
func NewEtcdManager() *EtcdManager {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2349"},
	})
	if err != nil {
		panic(err)
	}

	return &EtcdManager{
		client: client,
	}
}

// Run ...
func (m *EtcdManager) Run(ctx context.Context) {
	for {
		sess, err := concurrency.NewSession(m.client)
		if err != nil {
			m.logger.Error("concurrency.NewSessions", zap.Error(err))
			continue
		}

		election := concurrency.NewElection(sess, "/leader")

		ctx, cancel := context.WithCancel(ctx)

		ch := election.Observe(ctx)

		go func() {
		ObserverLoop:
			for res := range ch {
				for _, kv := range res.Kvs {
					num, err := strconv.Atoi(string(kv.Value))
					if err != nil {
						m.logger.Error("strconv.Atoi", zap.Error(err))
						break ObserverLoop
					}
					m.leaderChan <- LeaderEvent{
						Type:   EtcdEventTypePut,
						Leader: NodeID(num),
					}
				}
			}
			cancel()
		}()

		err = election.Campaign(ctx, "1")
		if err != nil {
			m.logger.Error("election.Campaign", zap.Error(err))
			cancel()
			continue
		}

		<-ctx.Done()

		err = election.Resign(context.Background())
		if err != nil {
			m.logger.Error("election.Resign", zap.Error(err))
			cancel()
			continue
		}
	}
}

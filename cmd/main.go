package main

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap/zapcore"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
	"txshard"

	"go.uber.org/zap"
)

func main() {
	if len(os.Args) <= 2 {
		panic("must provide node_id and last_partition")
	}

	num, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	nodeID := txshard.NodeID(num)

	num, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	lastPartition := txshard.PartitionID(num)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)

	zapConf := zap.NewProductionConfig()
	zapConf.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	logger, err := zapConf.Build()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		txshard.RunSystem(ctx, txshard.SystemConfig{
			AppName:        "sample",
			PartitionCount: 4,
			NodeID:         nodeID,
			LastPartition:  lastPartition,
			Runner: func(ctx context.Context, partitionID txshard.PartitionID) {
				logger.Info("Partition Started", zap.Any("partition.id", partitionID))
				<-ctx.Done()
				time.Sleep(2 * time.Second)
				logger.Info("Partition Stopped", zap.Any("partition.id", partitionID))
			},
			Logger: logger,
			EtcdConfig: clientv3.Config{
				Endpoints: []string{
					"localhost:2379",
				},
			},
		})
	}()

	<-done
	cancel()
	wg.Wait()
}

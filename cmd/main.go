package main

import (
	"context"
	"fmt"
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

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	manager := txshard.NewEtcdManager(logger)

	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	processorCtx, processorCancel := context.WithCancel(ctx)

	processor := txshard.NewProcessor(txshard.Config{
		PartitionCount:  4,
		PartitionPrefix: "/partition/",
		NodePrefix:      "/node/",

		SelfNodeID:        nodeID,
		SelfLastPartition: lastPartition,

		Client: manager,
		Runner: func(ctx context.Context, partitionID txshard.PartitionID) {
			fmt.Println("STARTED:", partitionID)
			<-ctx.Done()
			time.Sleep(2 * time.Second)
			fmt.Println("STOPPED:", partitionID)
		},
		Logger: logger,

		LeaseChan:     manager.GetLeaseChan(),
		NodeChan:      manager.WatchNodes(managerCtx, "/node/"),
		PartitionChan: manager.WatchPartitions(managerCtx, "/partition/"),
	})

	var processorWg sync.WaitGroup
	var managerWg sync.WaitGroup

	processorWg.Add(1)
	managerWg.Add(1)

	go func() {
		defer managerWg.Done()
		manager.Run(managerCtx)
		logger.Info("Etcd Manager stopped")
	}()

	go func() {
		defer processorWg.Done()
		processor.Run(processorCtx)
	}()

	<-done

	processorCancel()
	processorWg.Wait()

	managerCancel()
	managerWg.Wait()
}

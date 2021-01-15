package txshard

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"sync"
)

// SystemConfig ...
type SystemConfig struct {
	AppName        string
	PartitionCount PartitionID
	NodeID         NodeID
	LastPartition  PartitionID
	Runner         Runner

	Logger     *zap.Logger
	EtcdConfig clientv3.Config
}

// RunSystem ...
func RunSystem(rootCtx context.Context, conf SystemConfig) {
	manager := NewEtcdManager(conf.Logger, conf.EtcdConfig)

	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	processorCtx, processorCancel := context.WithCancel(ctx)

	partitionPrefix := "/" + conf.AppName + "/partition/"
	nodePrefix := "/" + conf.AppName + "/node/"

	processor := NewProcessor(ProcessorConfig{
		PartitionCount:  conf.PartitionCount,
		PartitionPrefix: partitionPrefix,
		NodePrefix:      nodePrefix,

		SelfNodeID:        conf.NodeID,
		SelfLastPartition: conf.LastPartition,

		Client: manager,
		Runner: conf.Runner,
		Logger: conf.Logger,

		LeaseChan:     manager.GetLeaseChan(),
		NodeChan:      manager.WatchNodes(managerCtx, nodePrefix),
		PartitionChan: manager.WatchPartitions(managerCtx, partitionPrefix),
	})

	var processorWg sync.WaitGroup
	var managerWg sync.WaitGroup

	processorWg.Add(1)
	managerWg.Add(1)

	go func() {
		defer managerWg.Done()
		manager.Run(managerCtx)
		conf.Logger.Info("Etcd Manager stopped")
	}()

	go func() {
		defer processorWg.Done()
		processor.Run(processorCtx)
	}()

	<-rootCtx.Done()

	processorCancel()
	processorWg.Wait()

	managerCancel()
	managerWg.Wait()
}

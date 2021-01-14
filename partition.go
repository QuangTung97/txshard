package txshard

type partitionActions struct {
	put    bool
	delete bool
	start  bool
	stop   bool
}

func computePartitionActions(
	isLeader bool, persisted bool, isSame bool, running bool,
) partitionActions {
	put := false
	deletePartition := false
	start := false
	stop := false

	if !isLeader {
		if running {
			stop = true
		}
		if persisted && isSame {
			deletePartition = true
		}
	} else {
		if !persisted {
			put = true
		} else {
			if isSame && !running {
				start = true
			}
		}
	}

	return partitionActions{
		put:    put,
		delete: deletePartition,
		start:  start,
		stop:   stop,
	}
}

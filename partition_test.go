package txshard

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestComputePartitionActions(t *testing.T) {
	table := []struct {
		name      string
		isLeader  bool
		persisted bool
		isSame    bool
		running   bool
		expected  partitionActions
	}{
		{
			name:     "not-leader.not-persisted.not-running",
			expected: partitionActions{},
		},
		{
			name:    "not-leader.not-persisted.running",
			running: true,
			expected: partitionActions{
				stop: true,
			},
		},
		{
			name:      "not-leader.persisted-not-same.not-running",
			persisted: true,
			isSame:    false,
			expected:  partitionActions{},
		},
		{
			name:      "not-leader.persisted-not-same.running",
			persisted: true,
			isSame:    false,
			running:   true,
			expected: partitionActions{
				stop: true,
			},
		},
		{
			name:      "not-leader.persisted-same.not-running",
			persisted: true,
			isSame:    true,
			expected: partitionActions{
				delete: true,
			},
		},
		{
			name:      "not-leader.persisted-same.running",
			persisted: true,
			isSame:    true,
			running:   true,
			expected: partitionActions{
				delete: true,
				stop:   true,
			},
		},
		{
			name:      "leader.not-persisted.not-running",
			isLeader:  true,
			persisted: false,
			running:   false,
			expected: partitionActions{
				put: true,
			},
		},
		{
			name:      "leader.not-persisted.running",
			isLeader:  true,
			persisted: false,
			running:   false,
			expected: partitionActions{
				put: true,
			},
		},
		{
			name:      "leader.persisted-not-same.not-running",
			isLeader:  true,
			persisted: true,
			isSame:    false,
			running:   false,
			expected:  partitionActions{},
		},
		{
			name:      "leader.persisted-not-same.running",
			isLeader:  true,
			persisted: true,
			isSame:    false,

			running:  true,
			expected: partitionActions{},
		},
		{
			name:      "leader.persisted-same.not-running",
			isLeader:  true,
			persisted: true,
			isSame:    true,
			running:   false,
			expected: partitionActions{
				start: true,
			},
		},
		{
			name:      "leader.persisted-same.running",
			isLeader:  true,
			persisted: true,
			isSame:    true,
			running:   true,
			expected:  partitionActions{},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			output := computePartitionActions(e.isLeader, e.persisted, e.isSame, e.running)
			assert.Equal(t, e.expected, output)
		})
	}
}

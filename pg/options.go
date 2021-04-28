package pg

import "time"

type Options struct {
	CompactionInterval time.Duration
	SyncInterval       time.Duration

	// perform a sync after every SetOffset
	SyncAfterOffset bool

	Recovery struct {
		BatchedOffsetSync time.Duration
	}
}

func DefaultOptions() *Options {
	return &Options{
		CompactionInterval: 10 * time.Second,
		SyncInterval:       1 * time.Second,
		Recovery: struct{ BatchedOffsetSync time.Duration }{
			BatchedOffsetSync: 10 * time.Second,
		},
	}
}

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
		CompactionInterval: 60 * time.Second,
		SyncInterval:       0,
		Recovery: struct{ BatchedOffsetSync time.Duration }{
			BatchedOffsetSync: 10 * time.Second,
		},
	}
}

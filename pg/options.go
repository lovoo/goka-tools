package pg

import "time"

type Options struct {
	CompactionInterval time.Duration
	SyncInterval       time.Duration

	JitterMax time.Duration

	// perform a sync after every SetOffset
	SyncAfterOffset bool

	Recovery struct {
		BatchedOffsetSync time.Duration

		// if true, will skip compaction during recovery
		NoCompaction bool
	}
}

func DefaultOptions() *Options {
	return &Options{
		CompactionInterval: 60 * time.Second,
		SyncInterval:       0,
		JitterMax:          10 * time.Second,
		Recovery: struct {
			BatchedOffsetSync time.Duration
			NoCompaction      bool
		}{
			BatchedOffsetSync: 10 * time.Second,
		},
	}
}

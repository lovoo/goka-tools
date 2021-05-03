package pg

import "time"

type Options struct {
	CompactionInterval time.Duration
	SyncInterval       time.Duration

	JitterMaxFraction float64

	// perform a sync after every SetOffset
	SyncAfterOffset bool

	Recovery struct {
		BatchedOffsetSync  time.Duration
		CompactionInterval time.Duration
	}
}

func DefaultOptions() *Options {
	return &Options{
		CompactionInterval: 60 * time.Second,
		SyncInterval:       30 * time.Second,
		JitterMaxFraction:  0.1,
		Recovery: struct {
			BatchedOffsetSync  time.Duration
			CompactionInterval time.Duration
		}{
			BatchedOffsetSync:  120 * time.Second,
			CompactionInterval: 180 * time.Second,
		},
	}
}

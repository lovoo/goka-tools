package dbtailer

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/pebble"
	"github.com/lovoo/goka"
)

type config struct {
	MaxItems    int64
	MaxAge      int64
	tmgrBuilder goka.TopicManagerBuilder
}

type Option func(c *config)

func MaxItems(maxItems int64) Option {
	return func(c *config) {
		c.MaxItems = maxItems
	}
}

func MaxAge(maxAge int64) Option {
	return func(c *config) {
		c.MaxAge = maxAge
	}
}

func TopicManagerBuilder(builder goka.TopicManagerBuilder) Option {
	return func(c *config) {
		c.tmgrBuilder = builder
	}
}

func (c *config) ensureDefaults() *config {
	if c == nil {
		c = &config{}
	}

	if c.tmgrBuilder == nil {
		c.tmgrBuilder = goka.DefaultTopicManagerBuilder
	}
	return c
}

type tailer struct {
	cfg *config
	db  *pebble.DB
}

type Tailer interface{}

func NewDBtailer(brokers []string, path string, topic string, opts ...Option) (Tailer, error) {
	cfg := &config{}

	for _, opt := range opts {
		opt(cfg)
	}
	cfg = cfg.ensureDefaults()

	pebOpts := &pebble.Options{
		DisableWAL: true,
	}

	topicManager, err := cfg.tmgrBuilder(brokers)
	if err != nil {
		return nil, fmt.Errorf("error creating topic manager: %v", err)
	}
	partitions, err := topicManager.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("error reading partitions for topic %s: %v", topic, err)
	}
	for _, partition := range partitions {
		offset, err := topicManager.GetOffset(topic, partition, sarama.OffsetOldest)
	}
	db, err := pebble.Open(path, pebOpts)
	if err != nil {
		return nil, fmt.Errorf("error creating backing database: %v", err)
	}

	return &tailer{
		cfg: cfg,
		db:  db,
	}, nil
}

func (t *tailer) Run(ctx context.Context) error {
	// determine partitions
}

package telly

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/multierr"

	rdb "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

// Telly imports from kafka to rethinkdb
type Telly struct {
	opts *options

	rsess rdb.QueryExecutor
}

// NewTelly creates a new telly importer
func NewTelly(ctx context.Context, executor rdb.QueryExecutor, dbName string,
	table string, inputTopic string, codec goka.Codec,
	options ...Option) (*Telly, error) {

	opts, err := createOptions(dbName, table, inputTopic, codec, options...)
	if err != nil {
		return nil, err
	}

	if err := createDbAndTable(ctx, executor, dbName, table, opts.primaryKeyName); err != nil {
		return nil, err
	}
	if err := createDbAndTable(ctx, executor, dbName, metaTable, ""); err != nil {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("error connecting to rdb: %v", err)
	}

	return &Telly{
		rsess: executor,
		opts:  opts,
	}, nil
}

// Run starts the importer and cleaner with passed brokers
func (t *Telly) Run(ctx context.Context, brokers []string) error {

	cons, err := t.opts.consBuilder(brokers, t.opts.clientID)
	if err != nil {
		return err
	}

	tmgr, err := t.opts.tmgrBuilder(brokers)
	if err != nil {
		return err
	}

	partitions, err := cons.Partitions(t.opts.topic)
	if err != nil {
		return fmt.Errorf("error listing partitions for topic %s: %v", t.opts.topic, err)
	}

	inputMsgChan := make(chan *sarama.ConsumerMessage, t.opts.inserterChanSize)

	// Load offsets for all partitions
	offsets, err := t.readOffsetDoc(ctx)
	if err != nil {
		return err
	}

	errg, ctx := multierr.NewErrGroup(ctx)
	for _, part := range partitions {
		offset, err := t.offsetForPartition(offsets, part, tmgr)
		if err != nil {
			return err
		}

		part := part
		errg.Go(func() error {
			return t.readPartition(ctx, part, cons, offset, inputMsgChan)
		})

	}

	errg.Go(func() error {
		return t.runInserter(ctx, inputMsgChan)
	})

	if t.opts.retention > 0 && t.opts.updateFieldName != "" {
		errg.Go(func() error {
			return t.runCleaner(ctx)
		})
	}

	return errg.Wait().ErrorOrNil()
}

func (t *Telly) Executor() rdb.QueryExecutor {
	return t.rsess
}

func (t *Telly) Table() rdb.Term {
	return rdb.DB(t.opts.dbName).Table(t.opts.table)
}

func (t *Telly) metaTable() rdb.Term {
	return rdb.DB(t.opts.dbName).Table(metaTable)
}

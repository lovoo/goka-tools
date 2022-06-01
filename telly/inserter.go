package telly

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
	rdb "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

const (
	maxBatch         = 1000 * time.Millisecond
	commitTimeout    = 2 * time.Second
	updatedTimestamp = "updated__"
	metaTable        = "telly_meta"
)

type offsetDoc struct {
	ID      string          `gorethink:"id"`
	Offsets map[int32]int64 `gorethink:"offsets"`
}

func (t *Telly) offsetKey() string {
	return t.opts.table
}

func (t *Telly) readOffsetDoc(ctx context.Context) (*offsetDoc, error) {
	cursor, err := t.metaTable().Get(t.offsetKey()).Run(t.rsess)
	if err != nil {
		return nil, fmt.Errorf("error reading stats:%v", err)
	}

	var offsets offsetDoc
	defer cursor.Close()
	if cursor.Next(&offsets) {
		if offsets.Offsets == nil {
			offsets.Offsets = make(map[int32]int64)
		}
		// fix invalid ID
		if offsets.ID != t.offsetKey() {
			offsets.ID = t.offsetKey()
		}
		return &offsets, nil
	}

	newOffsets := offsetDoc{
		ID:      t.offsetKey(),
		Offsets: make(map[int32]int64),
	}
	_, err = t.metaTable().Insert(&newOffsets, rethinkdb.InsertOpts{
		Conflict: "replace",
	}).RunWrite(t.rsess)
	return &newOffsets, err
}

func (t *Telly) runInserter(ctx context.Context, input <-chan *sarama.ConsumerMessage) error {
	offsets, err := t.readOffsetDoc(ctx)
	if err != nil {
		return fmt.Errorf("error reading stats: %v", err)
	}

	committer := time.NewTicker(maxBatch)
	defer committer.Stop()

	batch := make([]*sarama.ConsumerMessage, 0, t.opts.inserterBatchSize)

	var totalItemsCommitted int
	commit := func() {
		ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		defer cancel()

		newItems, updatedItems, _, err := t.insertBatch(ctx, offsets, batch)
		if err != nil {
			log.Printf("error committing batch: %v", err)
		}
		totalItemsCommitted += newItems + updatedItems

		committer.Reset(maxBatch)
		// reset batch
		batch = make([]*sarama.ConsumerMessage, 0, t.opts.inserterBatchSize)
	}

	defer commit()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-input:
			if !ok {
				return nil
			}
			batch = append(batch, msg)
			if len(batch) >= t.opts.inserterBatchSize {
				commit()
			}
		case <-committer.C:
			if len(batch) > 0 {
				commit()
			}
		}
	}
}

func (t *Telly) insertBatch(ctx context.Context, offsets *offsetDoc, batch []*sarama.ConsumerMessage) (int, int, int, error) {
	// ignore empty batch
	if len(batch) == 0 {
		return 0, 0, 0, nil
	}

	var (
		numItems       = len(batch)
		docs           = make([]interface{}, 0, numItems)
		deleteItemKeys []interface{}
	)

	for _, item := range batch {

		// nil-values
		if item.Value == nil {
			deleteItemKeys = append(deleteItemKeys, string(item.Key))
			continue
		}

		value, err := t.opts.codec.Decode(item.Value)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("error decoding: %v", err)
		}

		// execute hook
		value = t.opts.insertHook(item.Key, value)
		// check again for hooks, maybe the hook removes the value
		if value == nil {
			deleteItemKeys = append(deleteItemKeys, string(item.Key))
			continue
		}

		docs = append(docs, value)

		maxOffset := offsets.Offsets[item.Partition]
		// update offset (store the next offset though, otherwise we'd re-consume the same message again)
		if item.Offset+1 > maxOffset {
			offsets.Offsets[item.Partition] = item.Offset + 1
		}
	}

	insertResp, err := t.Table().Insert(docs, rethinkdb.InsertOpts{
		Conflict: "replace",
	}).RunWrite(t.rsess)
	if err != nil {
		return 0, 0, 0, err
	}

	// delete nil-values in the batch
	deleteResp, err := t.Table().
		GetAll(deleteItemKeys...).
		Delete(rdb.DeleteOpts{ReturnChanges: false, Durability: "soft", IgnoreWriteHook: true}).
		RunWrite(t.rsess)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error deleting nil-values: %v", err)
	}

	_, writeErr := t.metaTable().Insert(offsets, rethinkdb.InsertOpts{
		Conflict: "replace",
	}).RunWrite(t.rsess)

	return insertResp.Inserted, insertResp.Replaced, deleteResp.Deleted, writeErr
}

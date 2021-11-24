package telly

import (
	"context"
	"fmt"
	"log"
	"time"

	rdb "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func (t *Telly) runCleaner(ctx context.Context) error {

	ticker := time.NewTicker(t.opts.cleanInterval)
	defer ticker.Stop()

	if err := t.createSecondaryIndex(t.opts.updateFieldName); err != nil {
		return fmt.Errorf("error creating index on update-timetamp: %c", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			log.Printf("cleaning")
			fromTimestamp := time.Now().Add(-t.opts.retention)
			resp, err := t.Table().Between(0, fromTimestamp, rdb.BetweenOpts{
				Index: t.opts.updateFieldName,
			}).Delete(rdb.DeleteOpts{ReturnChanges: false, Durability: "soft", IgnoreWriteHook: true}).RunWrite(t.rsess)
			if err != nil {
				return fmt.Errorf("error cleaning: %v", err)
			}

			// TODO: metrics
			if resp.Deleted > 0 {
				log.Printf("deleted %d items", resp.Deleted)
			}
			ticker.Reset(t.opts.cleanInterval)
		}
	}
}

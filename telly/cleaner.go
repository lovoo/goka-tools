package telly

import (
	"context"
	"fmt"
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
			fromTimestamp := time.Now().Add(-t.opts.retention)
			_, err := t.Table().Between(0, fromTimestamp, rdb.BetweenOpts{
				Index: t.opts.updateFieldName,
			}).Delete(rdb.DeleteOpts{ReturnChanges: false, Durability: "soft", IgnoreWriteHook: true}).RunWrite(t.rsess)
			if err != nil {
				return fmt.Errorf("error cleaning: %v", err)
			}

			ticker.Reset(t.opts.cleanInterval)
		}
	}
}

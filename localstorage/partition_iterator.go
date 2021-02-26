package localstorage

import (
	"errors"
	"fmt"
	"log"

	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	// ErrStop indicates that the iteration should be estopped
	ErrStop = errors.New("Stop iteration requested")
)

// IterateStorage iterates multiple storage folders from a base path
// calling a handler for each key-value pair
func IterateStorage(basepath string, table string, partitions []int32, handleItem func(partition int32, key, value []byte) error) error {
	log.Printf("Iterating tables %s in path %s for partitions %+v", table, basepath, partitions)
	defer log.Printf("done.")
	opts := &opt.Options{
		ErrorIfMissing: true,
	}
	builder := storage.BuilderWithOptions(basepath, opts)
	for _, part := range partitions {
		b, err := builder(table, part)

		if err != nil {
			return fmt.Errorf("Error opening partition %s.%d: %v", table, part, err)
		}

		it, err := b.Iterator()
		if err != nil {
			return fmt.Errorf("Error creating iterator: %v", err)
		}

		defer func() {
			it.Release()
			if err = b.Close(); err != nil {
				log.Printf("Error closing iterator: %v", err)
			}
		}()

		for it.Next() {
			val, err := it.Value()
			if err != nil {
				return fmt.Errorf("error getting value for key %s: %v", string(it.Key()), err)
			}
			err = handleItem(part, it.Key(), val)

			if err != nil {
				if err == ErrStop {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

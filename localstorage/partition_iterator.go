package localstorage

import (
	"errors"
	"fmt"
	"log"

	"github.com/lovoo/goka/storage"
)

var (
	// ErrStop indicates that the iteration should be estopped
	ErrStop = errors.New("Stop iteration requested")
)

// IterateStorage iterates multiple storage folders from a base path
// calling a handler for each key-value pair
func IterateStorage(basepath string, table string, partitions int32, handleItem func(key, value []byte) error) error {
	log.Printf("Iterating table %s in path %s", table, basepath)
	defer log.Printf("done.")
	builder := storage.DefaultBuilder(basepath)
	for part := int32(0); part < partitions; part++ {
		b, err := builder(table, part)

		if err != nil {
			return fmt.Errorf("Error opening partition %s: %d", table, part)
		}

		it, err := b.Iterator()
		if err != nil {
			return fmt.Errorf("Error creating iterator: %v", err)
		}
		for it.Next() {
			val, err := it.Value()
			if err != nil {
				return fmt.Errorf("error getting value for key %s: %v", string(it.Key()), err)
			}
			err = handleItem(it.Key(), val)

			if err != nil {
				defer func() {
					it.Release()
					if err = b.Close(); err != nil {
						log.Printf("Error closin iterator: %v", err)
					}
				}()
				if err == ErrStop {
					return nil
				} else {
					return err
				}
			}
		}

		it.Release()
		if err = b.Close(); err != nil {
			return fmt.Errorf("Error closin iterator: %v", err)
		}

	}
	return nil
}

package pg

import (
	"fmt"
	"log"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/lovoo/goka/storage"
)

type sst struct {
	db     *pogreb.DB
	close  chan struct{}
	closed chan struct{}
}

// Build builds an sqlite storage for goka
func Build(path string) (storage.Storage, error) {

	db, err := pogreb.Open(path, &pogreb.Options{
		// BackgroundSyncInterval:       10 * time.Second,
		// BackgroundCompactionInterval: 15 * time.Second,
	})
	if err != nil {
		log.Fatalf("Error opening pogrep database: %v", err)
		return nil, nil
	}

	return &sst{
		db:     db,
		close:  make(chan struct{}),
		closed: make(chan struct{}),
	}, nil
}

func (s *sst) Open() error {
	_, err := s.db.Compact()
	return err
}

func (s *sst) Close() error {
	close(s.close)
	defer close(s.closed)
	return s.db.Close()
}
func (s *sst) Has(key string) (bool, error) {
	return false, nil
}

func (s *sst) Get(key string) ([]byte, error) {
	return s.db.Get([]byte(key))
}

func (s *sst) Set(key string, value []byte) error {
	return s.db.Put([]byte(key), value)
}

func (s *sst) Delete(key string) error {
	return nil
}

func (s *sst) GetOffset(def int64) (int64, error) {
	return 0, nil
}

func (s *sst) SetOffset(offset int64) error {
	return nil
}

func (s *sst) MarkRecovered() error {
	log.Printf("compacting initially")
	_, err := s.db.Compact()
	if err != nil {
		return err
	}
	log.Printf("...done")
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.close:
				return
			case <-s.closed:
				return
			case <-ticker.C:
				log.Printf("compacting")
				if err := s.db.Sync(); err != nil {
					log.Printf("error syncing: %v", err)
				}
				res, err := s.db.Compact()
				if err != nil {
					log.Printf("error compacting: %v", err)
				}
				log.Printf("...done: (segments %d, records %d)", res.CompactedSegments, res.ReclaimedRecords)
				ticker.Reset(5 * time.Second)
			}
		}
	}()
	return nil
}

func (s *sst) Iterator() (storage.Iterator, error) {
	return &iterator{
		it: s.db.Items(),
	}, nil
}

type iterator struct {
	it *pogreb.ItemIterator

	curKey []byte
	curVal []byte
	curErr error
}

func (i *iterator) Next() bool {
	i.curKey, i.curVal, i.curErr = i.it.Next()
	return i.curErr == nil

}
func (i *iterator) Err() error {
	return i.curErr
}
func (i *iterator) Key() []byte {
	return i.curKey
}
func (i *iterator) Value() ([]byte, error) {
	return i.curVal, i.Err()
}
func (i *iterator) Release() {
}
func (i *iterator) Seek(key []byte) bool {
	return false
}

func (s *sst) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	return nil, fmt.Errorf("not implemented")
}

package pg

import (
	"fmt"
	"log"

	"github.com/akrylysov/pogreb"
	"github.com/lovoo/goka/storage"
)

type sst struct {
	db *pogreb.DB
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
		db: db,
	}, nil
}

func (s *sst) Open() error {

	// go func() {
	// 	for {
	// 		time.Sleep(time.Second)
	// 		m := s.db.Metrics()
	// 		log.Printf("put %d, gets %d, dels %d, collisions  %d", m.Puts.Value(), m.Gets.Value(), m.Dels.Value(), m.HashCollisions.Value())
	// 	}
	// }()

	return nil
}

func (s *sst) Close() error {
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

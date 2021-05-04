package ldbstorage

import (
	"github.com/syndtr/goleveldb/leveldb"
	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	offsetKey = "__offset"
)

// iterator wraps an Iterator implementation and handles the value decoding and
// offset key skipping.
type iterator struct {
	iter ldbiter.Iterator
	snap *leveldb.Snapshot
}

func (i *iterator) Next() bool {
	next := i.iter.Next()
	if string(i.iter.Key()) == offsetKey {
		next = i.iter.Next()
	}

	return next
}

func (i *iterator) Err() error {
	return i.iter.Error()
}

func (i *iterator) Key() []byte {
	return i.iter.Key()
}

func (i *iterator) Value() ([]byte, error) {
	data := i.iter.Value()
	if data == nil {
		return nil, nil
	}

	return data, nil
}

func (i *iterator) Release() {
	i.iter.Release()
	i.snap.Release()
}

func (i *iterator) Seek(key []byte) bool {
	return i.iter.Seek(key)
}

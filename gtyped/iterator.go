package gtyped

import (
	"fmt"

	"github.com/lovoo/goka"
)

// GIterator is a typed variant of the iterator defined in goka.Iterator.
// See that type for details
type GIterator[K, V any] interface {
	Next() bool
	Err() error
	Key() K
	Value() (V, error)
	Release()
	Seek(key K) bool
}

type iterator[K, V any] struct {
	keyCodec GCodec[K]
	it       goka.Iterator
	err      error
}

func (it *iterator[K, V]) Next() bool {
	// once an error, always an error - there's no going back
	if it.err != nil {
		return false
	}

	return it.it.Next()
}

func (it *iterator[K, V]) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.it.Err()
}

func (it *iterator[K, V]) Key() K {
	encKey := it.it.Key()
	var zero K
	key, err := it.keyCodec.GDecode([]byte(encKey))
	if err != nil {
		it.err = fmt.Errorf("error decoding key: %w", err)
		return zero
	}
	return key
}

func (it *iterator[K, V]) Value() (V, error) {
	val, err := it.it.Value()
	var zero V
	if err != nil {
		return zero, err
	}
	if val == nil {
		return zero, nil
	}
	return val.(V), nil
}

func (it *iterator[K, V]) Release() {
	it.it.Release()
}

func (it *iterator[K, V]) Seek(key K) bool {
	encKey, err := it.keyCodec.GEncode(key)
	if err != nil {
		it.err = fmt.Errorf("error decoding key: %w", err)
		return false
	}
	return it.it.Seek(string(encKey))
}

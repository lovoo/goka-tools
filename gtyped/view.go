package gtyped

//

import (
	"context"
	"fmt"

	"github.com/lovoo/goka"
)

type GView[K, V any] interface {
	Get(key K) (V, error)
	Has(key K) (bool, error)
	Recovered() bool
	Iterator() (GIterator[K, V], error)
	Evict(key K) error
	Run(context.Context) error
}

type gView[K, V any] struct {
	keyCodec GCodec[K]
	view     *goka.View
}

// NewGView creates a new view by wrapping a goka-View in a typed fashion.
// Note that it supports both key and value codecs.
// Goka views traditionally treat keys as string. To avoid having to define two codecs, use
// NewGStringView instead, which only requires a value-codec.
func NewGView[K, V any](brokers []string, table goka.Table, keyCodec GCodec[K], valueCodec GCodec[V], options ...goka.ViewOption) (GView[K, V], error) {
	valueBridge := NewCodecAdapter(valueCodec)
	view, err := goka.NewView(brokers, table, valueBridge, options...)
	if err != nil {
		return nil, err
	}

	return &gView[K, V]{
		keyCodec: keyCodec,
		view:     view,
	}, nil
}

func NewGStringView[V any](brokers []string, table goka.Table, valueCodec GCodec[V], options ...goka.ViewOption) (GView[string, V], error) {
	return NewGView[string](brokers, table, new(StringCodec[string]), valueCodec, options...)
}

func (gv *gView[K, V]) Get(key K) (V, error) {
	var zero V
	encKey, err := gv.keyCodec.GEncode(key)
	if err != nil {
		return zero, fmt.Errorf("error encoding key: %w", err)
	}
	val, err := gv.view.Get(string(encKey))
	if err != nil {
		return zero, err
	}
	if val == nil {
		return zero, nil
	}
	return val.(V), nil
}

func (gv *gView[K, V]) Has(key K) (bool, error) {
	encKey, err := gv.keyCodec.GEncode(key)
	if err != nil {
		return false, fmt.Errorf("error encoding key: %w", err)
	}
	return gv.view.Has(string(encKey))
}

func (gv *gView[K, V]) Recovered() bool {
	return gv.view.Recovered()
}

func (gv *gView[K, V]) Iterator() (GIterator[K, V], error) {
	it, err := gv.view.Iterator()
	if err != nil {
		return nil, err
	}
	return &iterator[K, V]{
		it: it,
	}, nil
}

func (gv *gView[K, V]) Evict(key K) error {
	encKey, err := gv.keyCodec.GEncode(key)
	if err != nil {
		return fmt.Errorf("error encoding key: %w", err)
	}
	return gv.view.Evict(string(encKey))
}

func (gv *gView[K, V]) Run(ctx context.Context) error {
	return gv.view.Run(ctx)
}

package gtyped

import (
	"fmt"

	"github.com/lovoo/goka"
)

type GCodec[V any] interface {
	GEncode(value V) (data []byte, err error)
	GDecode(data []byte) (value V, err error)
}

type codecBridge[T any] struct {
	c GCodec[T]
}

func (cb *codecBridge[T]) Encode(value interface{}) ([]byte, error) {
	tVal, ok := value.(T)
	var zero T
	if !ok {
		return nil, fmt.Errorf("unexpected type while encoding. Expected %T, got %T", zero, value)
	}
	return cb.c.GEncode(tVal)
}

func (cb *codecBridge[T]) Decode(data []byte) (interface{}, error) {
	return cb.c.GDecode(data)
}

// CodecBridge converts a untyped (goka) codec to a typed codec
func NewCodecBridge[T any](codec GCodec[T]) goka.Codec {
	return &codecBridge[T]{
		c: codec,
	}
}

type StringCodec[T ~string] struct{}

func (s *StringCodec[T]) GEncode(value T) (data []byte, err error) {
	return []byte(string(value)), nil
}

func (s *StringCodec[T]) GDecode(data []byte) (T, error) {
	return T(string(data)), nil
}

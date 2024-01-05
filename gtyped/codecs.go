package gtyped

import (
	"fmt"

	"github.com/lovoo/goka"
)

type GCodec[V any] interface {
	GEncode(value V) (data []byte, err error)
	GDecode(data []byte) (value V, err error)
}

type codecAdapter[T any] struct {
	c GCodec[T]
}

func (cb *codecAdapter[T]) Encode(value any) ([]byte, error) {
	tVal, ok := value.(T)
	var zero T
	if !ok {
		return nil, fmt.Errorf("unexpected type while encoding. Expected %T, got %T", zero, value)
	}
	return cb.c.GEncode(tVal)
}

func (cb *codecAdapter[T]) Decode(data []byte) (any, error) {
	return cb.c.GDecode(data)
}

// CodecAdapter adapts a typed gtyped.Codec to an untyped (goka) codec
func NewCodecAdapter[T any](codec GCodec[T]) goka.Codec {
	return &codecAdapter[T]{
		c: codec,
	}
}

type StringCodec[T ~string] struct{}

func (s *StringCodec[T]) GEncode(value T) ([]byte, error) {
	return []byte(string(value)), nil
}

func (s *StringCodec[T]) GDecode(data []byte) (T, error) {
	return T(string(data)), nil
}

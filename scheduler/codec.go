package scheduler

import (
	"fmt"

	"github.com/lovoo/goka"
	"google.golang.org/protobuf/proto"
)

// OrderCodec allows to marshal and unmarshal items of type Order
type OrderCodec struct{}

// Decode provides unmarshals a Order json into the struct.
func (c *OrderCodec) Decode(data []byte) (interface{}, error) {

	var target Order
	err := proto.Unmarshal(data, &target)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling value into Order: %v", err)
	}

	return &target, nil
}

// Encode marshals a Order
func (c *OrderCodec) Encode(value interface{}) ([]byte, error) {
	target, isOrder := value.(*Order)
	if !isOrder {
		return nil, fmt.Errorf("Error encoding value. Expected *Order but got %T", value)
	}

	return proto.Marshal(target)
}

// NewOrderCodec creates a new codec for encoding/decoding a Order
func NewOrderCodec() goka.Codec {
	return new(OrderCodec)
}

// WaitCodec allows to marshal and unmarshal items of type Wait
type WaitCodec struct{}

// Decode provides unmarshals a Wait json into the struct.
func (c *WaitCodec) Decode(data []byte) (interface{}, error) {

	var target Wait
	err := proto.Unmarshal(data, &target)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling value into Wait: %v", err)
	}

	return &target, nil
}

// Encode marshals a Wait
func (c *WaitCodec) Encode(value interface{}) ([]byte, error) {
	target, isWait := value.(*Wait)
	if !isWait {
		return nil, fmt.Errorf("Error encoding value. Expected *Wait but got %T", value)
	}

	return proto.Marshal(target)
}

// NewWaitCodec creates a new codec for encoding/decoding a Wait
func NewWaitCodec() goka.Codec {
	return new(WaitCodec)
}

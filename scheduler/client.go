package scheduler

import (
	"fmt"
	"time"

	"github.com/lovoo/goka"
	"github.com/rs/xid"
)

// CreateDelayedOrderWithPrefix allows to create an order to be sent to the scheduler.
// The order's destination time is defined by a delay, so there will be an implicit catchup.
func CreateDelayedOrderWithPrefix(targetKey []byte, targetTopic string, variant string, message []byte, orderType OrderType, delay time.Duration) (string, *Order, error) {

	var key string
	switch orderType {
	case OrderType_Delay:
		key = xid.New().String()
	case OrderType_ThrottleFirst, OrderType_ThrottleFirstReschedule:
		suffix := ""
		if variant != "" {
			suffix = "-" + variant
		}
		key = fmt.Sprintf("%s-%s%s", targetTopic, string(targetKey), suffix)
	default:
		return "", nil, fmt.Errorf("Invalid/Unsupported order type: %#v", orderType)
	}

	order := &Order{
		Payload: &Order_Payload{
			Key:     targetKey,
			Topic:   targetTopic,
			Message: message,
		},
		DelayMs:   delay.Milliseconds(),
		OrderType: orderType,
	}
	return key, order, order.validate()
}

// OrderClient helps communicating with a scheduler to place orders
type OrderClient interface {
	// NewOrder creates a new order
	NewOrder(targetKey []byte, targetTopic string, variant string, message []byte, orderType OrderType, delay time.Duration) (string, *Order, error)

	// OrderEdge returns the group graph edge that can be used to client procesors
	// to be able to place orders into the scheduler. Like this:
	// goka.NewProcessor(..., goka.DefineGroup(..., om.OrderEdge()))
	OrderEdge() goka.Edge

	// OrderTopic returns the topic to be used for the emitting orders to the scheduler.
	OrderTopic() goka.Stream
}

type orderClient struct {
	topicPrefix string
	inputStream goka.Stream
}

func NewOrderClient(topicPrefix string) OrderClient {
	return &orderClient{
		topicPrefix: topicPrefix,
		inputStream: goka.Stream(orderStream.get(topicPrefix)),
	}
}

func (o *orderClient) NewOrder(targetKey []byte, targetTopic string, variant string, message []byte, orderType OrderType, delay time.Duration) (string, *Order, error) {
	return CreateDelayedOrderWithPrefix(targetKey, targetTopic, variant, message, orderType, delay)
}

// OrderEdge returns the group graph edge that can be used to client procesors
// to be able to place orders into the scheduler. Like this:
// goka.NewProcessor(..., goka.DefineGroup(..., om.OrderEdge()))
func (o *orderClient) OrderEdge() goka.Edge {
	return goka.Output(o.inputStream, NewOrderCodec())
}

func (o *orderClient) OrderTopic() goka.Stream {
	return o.inputStream
}

package scheduler

import (
	"fmt"
)

func (o *Order) validate() error {

	// nop ayload
	if o.GetPayload() == nil {
		return fmt.Errorf("Order does not have a payload")
	}
	// empty key
	if len(o.Payload.Key) == 0 {
		return fmt.Errorf("An order's target key cannot be empty")
	}

	// empty topic
	if len(o.Payload.Topic) == 0 {
		return fmt.Errorf("An order's target topic cannot be empty")
	}

	// invalid order type
	if _, ok := OrderType_name[int32(o.OrderType)]; o.OrderType == 0 || !ok {
		return fmt.Errorf("Order type is invalid: %v", o.OrderType)
	}

	// neither execution time nor delay set
	if o.ExecutionTime.AsTime().IsZero() {
		if o.DelayMs < 0 {
			return fmt.Errorf("Cannot execute order with negative delay_ms without exec_time")
		}
	}
	return nil
}

// Inc increases the iterations counter
func (w *Wait) Inc() *Wait {
	w.Iterations++
	return w
}

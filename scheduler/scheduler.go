package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/golang/protobuf/ptypes"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
)

const (
	// group name of the scheduler
	orderGroup prefixable = "scheduler-order"
	// orderStream to place orders in the scheduler
	orderStream prefixable = "scheduler-place-order"
	// orderStream to tell the scheduler to execute if the destination time is due
	executeStream prefixable = "scheduler-execute"

	// waiter group template
	waiterGroup prefixAndTime = "scheduler-waiter"
	// waiter delay input stream (i.e. push to the topic to make the waiter waiting for that time.
	waiterDelayStream prefixAndTime = "scheduler-waiter-delay"

	// if we receive orders with execution times in the past by that much
	// we'll consider it a "catchup" and have to decide wether we do want to execute it.
	orderCatchupTimeout = 10 * time.Second
)

type prefixable string

func (p prefixable) get(prefix string) string {
	if prefix != "" {
		return fmt.Sprintf("%s-%s", prefix, p)
	}
	return string(p)
}

type prefixAndTime string

func (pt prefixAndTime) get(prefix string, timeDef int64) string {
	if prefix != "" {
		return fmt.Sprintf("%s-%s-%d", prefix, pt, timeDef)
	}
	return fmt.Sprintf("%s-%d", pt, timeDef)
}

var (
	schedLog = logger.Default().Prefix("goka-tools-scheduler")
)

// Emitter is a generic emitter, but probably a *goka.Emitter
// Note that the emitter must be closed (by calling Finish) by the creator, the scheduler
// is not capable of closing the emitters, because it does not have an internal state.
type Emitter interface {
	Emit(key string, msg interface{}) (*goka.Promise, error)
	Finish() error
}

// EmitterCreator is a callback that will be used by the scheduler to create
// emitters on the fly while receiving orders.
type EmitterCreator func(goka.Stream, goka.Codec) (Emitter, error)

var (
	clk = clock.New()
)

type waiters struct {
	ws            []*waiter
	executeStream goka.Stream
	config        *Config
}

func (ws *waiters) reschedule(ctx goka.Context, wait *Wait) bool {
	// select the responsible waiter
	// emit the message to the schedulers
	waitDuration := wait.ExecutionTime.AsTime().Sub(clk.Now())

	waiterIdx := sort.Search(len(ws.ws), func(i int) bool {
		return waitDuration >= ws.ws[i].maxWaitTime
	})

	// didn't find a waiter, that means the execution time is smaller than the smallest
	// waiter period. So let's execute right away
	if waiterIdx >= len(ws.ws) {
		return false
	}

	wait.EnterQueueTime, _ = ptypes.TimestampProto(clk.Now())

	// we have a valid waiter, resend the message to this one
	ws.config.mxRescheduled(fmt.Sprintf("%d", ws.ws[waiterIdx].maxWaitTime.Milliseconds()), 1)
	ctx.Emit(ws.ws[waiterIdx].topic, ctx.Key(), wait.Inc())
	return true
}

func (ws *waiters) createGraphs() []*goka.GroupGraph {
	var waiterGraphs []*goka.GroupGraph
	for _, w := range ws.ws {
		// generate the edges for the group graph
		var edges = []goka.Edge{
			goka.Input(w.topic, NewWaitCodec(), w.wait),
			goka.Output(ws.executeStream, NewWaitCodec()),
		}
		for _, w2 := range ws.ws {
			edges = append(edges, goka.Output(w2.topic, NewWaitCodec()))
		}

		waiterGraphs = append(waiterGraphs,
			goka.DefineGroup(w.group,
				edges...,
			),
		)
	}
	return waiterGraphs
}

// Scheduler implements a scheduler
type Scheduler struct {
	mOutputs       sync.RWMutex
	inputStream    goka.Stream
	outputs        map[string]Emitter
	waiters        *waiters
	emitterCreator EmitterCreator
	config         *Config
	topicPrefix    string
}

type waiter struct {
	maxWaitTime time.Duration
	group       goka.Group
	topic       goka.Stream
	waiters     *waiters
}

// CreateScheduler creates a scheduler type that provides the group graphs for all included
// components.
// To be independent of the processor handling boilerplate code, creating the group graphs is sufficient
// waitIntervals is a slice of time.Duration, creating a wait-processor for each wait interval.
// Note that the duration will be truncated to milliseconds and the list deduplicated automatically.
func CreateScheduler(config *Config, waitIntervals []time.Duration, creator EmitterCreator, topicPrefix string) *Scheduler {
	return &Scheduler{
		outputs:        make(map[string]Emitter),
		emitterCreator: creator,
		inputStream:    goka.Stream(orderStream.get(topicPrefix)),
		waiters:        createWaiters(config, waitIntervals, topicPrefix),
		config:         config,
		topicPrefix:    topicPrefix,
	}
}

// CreateGraphs creates the group graphs for all components used for the scheduler:
// one for the scheduler itself (that does the deduplication and store the payload)
// one for each waiter, i.e. one for each interval.
func (s *Scheduler) CreateGraphs() []*goka.GroupGraph {
	// collect all edges for the scheduler
	var schedEdges = []goka.Edge{
		goka.Persist(NewOrderCodec()),
		goka.Input(s.inputStream, NewOrderCodec(), s.placeOrder),
		goka.Input(goka.Stream(executeStream.get(s.topicPrefix)), NewWaitCodec(), s.requestExecute),
		goka.Loop(NewOrderCodec(), s.executeOrder),
	}
	for _, w := range s.waiters.ws {
		schedEdges = append(schedEdges, goka.Output(w.topic, NewWaitCodec()))
	}

	return append([]*goka.GroupGraph{
		goka.DefineGroup(goka.Group(orderGroup.get(s.topicPrefix)), schedEdges...),
	}, s.waiters.createGraphs()...)

}

// Close finishes all created emitters.
// Note that the processors are not stopped, because it is started by the client
func (s *Scheduler) Close() error {
	s.mOutputs.Lock()
	defer s.mOutputs.Unlock()

	var (
		wg   sync.WaitGroup
		errs = new(multierr.Errors)
	)

	for _, emitter := range s.outputs {
		wg.Add(1)
		emitter := emitter
		go func() {
			defer wg.Done()
			errs.Collect(emitter.Finish())
		}()
	}
	wg.Wait()

	if err := errs.NilOrError(); err != nil {
		return fmt.Errorf("Error closing scheduler emitters: %v", err)
	}
	return nil
}

func createWaiters(config *Config, waitIntervals []time.Duration, topicPrefix string) *waiters {
	// sort and deduplicate the wait intervals
	sortedIntervals := makeSortedIntervalList(waitIntervals)

	// create the waiters struct and create a waiter for each interval
	waiters := &waiters{
		executeStream: goka.Stream(executeStream.get(topicPrefix)),
		config:        config,
	}
	for _, interval := range sortedIntervals {
		w := &waiter{
			group:       goka.Group(waiterGroup.get(topicPrefix, int64(interval.Milliseconds()))),
			topic:       goka.Stream(waiterDelayStream.get(topicPrefix, int64(interval.Milliseconds()))),
			maxWaitTime: interval,
		}

		waiters.ws = append(waiters.ws, w)
	}

	// assign all waiters to each waiter
	for _, w := range waiters.ws {
		w.waiters = waiters
	}

	return waiters
}

func makeSortedIntervalList(intervals []time.Duration) []time.Duration {
	var (
		intervalSet = make(map[time.Duration]bool)
		sorted      []time.Duration
	)
	for _, interval := range intervals {
		// we don't allow intervals shorter than a millisecond
		interval = interval.Truncate(time.Millisecond)

		if interval == 0 {
			schedLog.Printf("WARNING: Wait times smaller than 1ms are not supported. Ignoring wait time.")
			continue
		}

		if intervalSet[interval] {
			schedLog.Printf("WARNING: The interval list contains a duplicate for %dms", interval.Milliseconds())
			continue
		}
		intervalSet[interval] = true
		sorted = append(sorted, interval)

	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] > sorted[j]
	})
	return sorted
}

func (w *waiter) wait(ctx goka.Context, msg interface{}) {
	// check due time of the order
	// check my own sleep time, sleep accordingly
	// forward to the next one or back to the scheduler
	wait := msg.(*Wait)
	delay := wait.ExecutionTime.AsTime().Sub(clk.Now())

	if delay >= w.maxWaitTime {
		delay = w.maxWaitTime
	}

	// substract the time that the message has already waited implicitly by being in the queue
	if wait.EnterQueueTime != nil {
		delay = delay - clk.Now().Sub(wait.EnterQueueTime.AsTime())
	} else {
		// we didn't set the enter queue time, let's reschedule immediately
		delay = 0
	}

	// only sleep if the time isn't negative
	if delay > 0 {
		// wait fort the
		timer := clk.Timer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Context().Done():
		}
	}
	if !w.waiters.reschedule(ctx, wait) {
		ctx.Emit(w.waiters.executeStream, ctx.Key(), wait)
	}
}

func (s *Scheduler) placeOrder(ctx goka.Context, msg interface{}) {
	newOrder := msg.(*Order)
	// validate order and drop if in valid
	if err := newOrder.validate(); err != nil {
		schedLog.Printf("Ignoring request for invalid order: %v", err)
		return
	}

	// try to create an emitter before we "accept" the order, so we don't end up failing when we're supposed to execute the order in the end.
	_, err := s.getOrCreateEmitter(newOrder.Payload.Topic)
	if err != nil {
		ctx.Fail(fmt.Errorf("Cannot create an emitter for target %s: %v", newOrder.Payload.Topic, err))
	}

	s.config.mxPlaceOrderLag(clk.Since(ctx.Timestamp()).Seconds())

	// set execution time if it's not specified or zero or something
	if newOrder.ExecutionTime == nil || newOrder.ExecutionTime.AsTime().Unix() == 0 || newOrder.ExecutionTime.AsTime().IsZero() {
		newOrder.ExecutionTime, _ = ptypes.TimestampProto(clk.Now().Add(time.Duration(newOrder.DelayMs) * time.Millisecond))
	}

	var order *Order
	if orderVal := ctx.Value(); orderVal != nil {
		order = orderVal.(*Order)
	}

	switch newOrder.OrderType {
	case OrderType_Delay:
		if order != nil {
			schedLog.Printf("duplicate DELAY order. Each delay needs a unique key. Dropping the new order.")
			return
		}
		if clk.Now().Sub(newOrder.ExecutionTime.AsTime()) > s.config.orderCatchupTimeout {
			if !newOrder.NoCatchup {
				s.executeOrder(ctx, newOrder)
			}
			return
		}
		ctx.SetValue(newOrder)

		if !s.waiters.reschedule(ctx, &Wait{ExecutionTime: newOrder.ExecutionTime}) {
			s.executeOrder(ctx, newOrder)
		}
	case OrderType_ThrottleFirst, OrderType_ThrottleFirstReschedule:
		// if we find an order whose execution time is way too old,
		// we'll delete it and create a new one because we assume it must have gotten lost
		// from broken waiters/changing intervals etc.
		if order != nil && order.ExecutionTime.AsTime().Before(clk.Now().Add(-s.config.orderZombieTTL)) {
			order = nil
			ctx.Delete()
			s.config.mxZombieEvicted(1)
		}

		// we have a order already, so we
		if order != nil {
			// (a) reschedule if the new order is configured to reschedule
			// AND the new order's exec time is closer,
			if newOrder.OrderType == OrderType_ThrottleFirstReschedule &&
				newOrder.ExecutionTime.AsTime().Before(order.ExecutionTime.AsTime()) {
				s.config.mxThrottleFirstRescheduled(1)
				ctx.SetValue(newOrder)
				if !s.waiters.reschedule(ctx, &Wait{ExecutionTime: newOrder.ExecutionTime}) {
					s.executeOrder(ctx, newOrder)
				}
				return
			}

			// (b) drop it as it is a complete duplicate
			// it's a valid duplicate, so we'll measure it and drop it.
			s.config.mxThrottleDuplicate(1)

			return
		} else {
			// if we're past execution time (plus some timeout value), and catchup is not ignored: execute immediately
			if clk.Now().Sub(newOrder.ExecutionTime.AsTime()) > s.config.orderCatchupTimeout {
				if !newOrder.NoCatchup {
					s.executeOrder(ctx, newOrder)
				}
				return
			}
			ctx.SetValue(newOrder)
			if !s.waiters.reschedule(ctx, &Wait{ExecutionTime: newOrder.ExecutionTime}) {
				s.executeOrder(ctx, newOrder)
			}
		}
	default:
		ctx.Fail(fmt.Errorf("unimplemented order type %v", newOrder.OrderType))
	}
}

func (s *Scheduler) getOrCreateEmitter(topic string) (Emitter, error) {
	// check if we have the emitter and return it if we have
	s.mOutputs.RLock()
	emitter := s.outputs[topic]
	if emitter != nil {
		s.mOutputs.RUnlock()
		return emitter, nil
	}

	s.mOutputs.RUnlock()

	// if we don't have, write-lock, check again and then recreate
	// (or return the copy that might have been created in the unlocked block)
	s.mOutputs.Lock()
	defer s.mOutputs.Unlock()

	emitter = s.outputs[topic]
	if emitter != nil {
		return emitter, nil
	}

	emitter, err := s.emitterCreator(goka.Stream(topic), new(codec.Bytes))
	if err != nil {
		return nil, fmt.Errorf("Error creating emitter: %v", err)
	}

	s.outputs[topic] = emitter
	return emitter, nil
}

func (s *Scheduler) requestExecute(ctx goka.Context, msg interface{}) {

	orderVal := ctx.Value()
	if orderVal == nil {
		s.config.mxExecutionDropped(1)
		return
	}
	order := orderVal.(*Order)

	// send the order execution via kafka one more time so we achieve at least once semantics
	ctx.Loopback(ctx.Key(), order)

	wait := msg.(*Wait)
	s.config.mxExecuteRoundTrips(float64(wait.Iterations))
	// we pushed the order to its final execution-request, so delete it to get ready for the next order
	ctx.Delete()
}

// actually executes the order by emitting into
func (s *Scheduler) executeOrder(ctx goka.Context, msg interface{}) {
	order := msg.(*Order)

	if err := order.validate(); err != nil {
		schedLog.Printf("Ignoring execution of invalid order: %v", err)
		return
	}

	emitter, err := s.getOrCreateEmitter(order.Payload.Topic)

	if err != nil {
		ctx.Fail(fmt.Errorf("Cannot create emitter for order %#v: %v", order, err))
	}

	s.config.mxExecutionTimeDeviation(clk.Since(order.ExecutionTime.AsTime()).Seconds())

	prom, err := emitter.Emit(string(order.Payload.Key), []byte(order.Payload.Message))

	if err != nil {
		ctx.Fail(fmt.Errorf("Cannot emit order (key=%s, topic=%s): %v", order.Payload.Key, order.Payload.Topic, err))
	}

	// mark the callback as not done yet
	commit := ctx.DeferCommit()

	// mark the callback as done asynchronously
	prom.Then(func(err error) {
		if err != nil {
			commit(fmt.Errorf("Cannot emit order (key=%s, topic=%s): %v", order.Payload.Key, order.Payload.Topic, err))
		} else {
			commit(nil)
		}
	})
}

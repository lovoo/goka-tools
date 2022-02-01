package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka-tools/mock"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/tester"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestScheduler(t *testing.T) {

	t.Run("create-graphs", func(t *testing.T) {
		sched := CreateScheduler(NewConfig(), []time.Duration{10 * time.Microsecond, 100 * time.Second}, func(goka.Stream, goka.Codec) (Emitter, error) {
			return nil, nil
		}, "mypref")

		graphs := sched.CreateGraphs()
		schedGraph := graphs[0]
		if !strings.HasPrefix(string(schedGraph.Group()), "mypref") {
			t.Errorf("group is not prefixed")
		}

		if !strings.HasPrefix(schedGraph.GroupTable().Topic(), "mypref") {
			t.Errorf("waiter group table is not prefixed")
		}

		// check the remaining group graphs
		for _, waiter := range graphs[1:] {
			if !strings.HasPrefix(string(waiter.Group()), "mypref") {
				t.Errorf("waiter group is not prefixed")
			}
		}
	})
}

func TestPrefixable(t *testing.T) {
	p := prefixable("a")
	if p.get("") != "a" {
		t.Errorf("no prefix shouldn't change the value")
	}
	if p.get("b") != "b-a" {
		t.Errorf("prefix does not work")
	}

	pt := prefixAndTime("a")

	if pt.get("", 0) != "a-0" {
		t.Errorf("no prefix should work")
	}
	if pt.get("b", 500) != "b-a-500" {
		t.Errorf("prefix with time should work")
	}
}

func TestWaiters(t *testing.T) {

	cfg := NewConfig()

	t.Run("create-waiters", func(t *testing.T) {

		// create waiters without duration.
		ws := createWaiters(cfg, []time.Duration{}, "my")

		if len(ws.ws) != 0 {
			t.Errorf("waiters without wait time should be empty")
		}

		// create waiters with some mix of durations out of order
		ws = createWaiters(cfg, []time.Duration{1 * time.Second,
			5 * time.Second,
			2 * time.Second,
			100 * time.Millisecond,
			100500 * time.Microsecond, // wait times will be truncated to millisecond, so this is the same as 100ms
			5 * time.Second,
		}, "my")

		// after creation, they're sorted descending (longest first)
		for i, wt := range []time.Duration{5 * time.Second, 2 * time.Second, 1 * time.Second, 100 * time.Millisecond} {
			if ws.ws[i].maxWaitTime != wt {
				t.Errorf("unexpected waiting time %v (expected %v)", ws.ws[i].maxWaitTime, wt)
			}
		}

		if ws.ws[0].topic != goka.Stream(waiterDelayStream.get("my", 5000)) {
			t.Errorf("unexpected waiter topic")
		}
	})

	t.Run("reschedule", func(t *testing.T) {
		ctx := mock.NewGokaMockContext(t).WithKeyValue("key", nil)
		ws := createWaiters(cfg, []time.Duration{1 * time.Second,
			5 * time.Second, 2 * time.Second,
			100 * time.Millisecond,
			5 * time.Second},
			"my")

		for idx, test := range []struct {
			dest         time.Time
			expectedEmit goka.Stream
		}{
			{
				dest:         clk.Now(),
				expectedEmit: ws.executeStream,
			},
			{
				dest:         clk.Now().Add(1001 * time.Millisecond),
				expectedEmit: "my-scheduler-waiter-delay-1000",
			},
			{

				dest:         clk.Now().Add(1000 * time.Millisecond),
				expectedEmit: "my-scheduler-waiter-delay-100",
			},
			{
				dest:         clk.Now().Add(1000 * time.Second),
				expectedEmit: "my-scheduler-waiter-delay-5000",
			},
			{
				dest:         clk.Now().Add(100 * time.Millisecond),
				expectedEmit: ws.executeStream,
			},
		} {
			ctx.Reset()
			execTime := timestamppb.New(test.dest)
			wait := &Wait{ExecutionTime: execTime}
			if !ws.reschedule(ctx, wait) {
				ctx.Emit(ws.executeStream, ctx.Key(), wait)
			}

			if len(ctx.GetEmitForTopic(test.expectedEmit)) != 1 {
				t.Errorf("expected emit in stream %s in test %d (emits are %#v)", test.expectedEmit, idx, ctx.GetAllEmits())
			}
		}
	})
}

type testOrder struct {
	key   string
	order *Order
}

func mustTestOrder(key string, order *Order, err error) *testOrder {
	if err != nil {
		panic(err)
	}
	return &testOrder{
		key:   key,
		order: order,
	}
}

func TestSchedulerIntegration(t *testing.T) {
	gkt := tester.New(t)

	emitterTester := tester.New(t)

	mockClk := clock.NewMock()
	clk = mockClk

	targetTracker := emitterTester.NewQueueTracker("target")

	emitter1, err := goka.NewEmitter(nil, "scheduler-waiter-delay-1000", NewWaitCodec(), goka.WithEmitterTester(gkt))
	if err != nil {
		t.Errorf("Error creating emitter: %v", err)
	}
	emitter5, err := goka.NewEmitter(nil, "scheduler-waiter-delay-5000", NewWaitCodec(), goka.WithEmitterTester(gkt))
	if err != nil {
		t.Errorf("Error creating emitter: %v", err)
	}

	targetEmitter, err := goka.NewEmitter(nil, "target", new(codec.Bytes), goka.WithEmitterTester(emitterTester))
	if err != nil {
		t.Errorf("Error creating emitter: %v", err)
	}

	emitters := map[goka.Stream]*goka.Emitter{
		"scheduler-waiter-1000": emitter1,
		"scheduler-waiter-5000": emitter5,
		"target":                targetEmitter,
	}

	var emitterFactory EmitterCreator = func(stream goka.Stream, _ goka.Codec) (Emitter, error) {
		// extract the time suffix from the stream so we can
		emitter := emitters[stream]
		if emitter == nil {
			return nil, fmt.Errorf("cannot find emitter for stream: %s", stream)
		}
		return emitter, nil
	}

	sched := CreateScheduler(NewConfig(), []time.Duration{1 * time.Second, 5 * time.Second}, emitterFactory, "")

	var procs []*goka.Processor
	for _, graph := range sched.CreateGraphs() {

		proc, err := goka.NewProcessor(nil, graph, goka.WithTester(gkt))
		if err != nil {
			t.Errorf("Error creating scheduler processor %v", err)
		}
		procs = append(procs, proc)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errg, ctx := multierr.NewErrGroup(ctx)

	for _, proc := range procs {
		wp := proc
		errg.Go(func() error {
			return wp.Run(ctx)
		})
	}

	// shut it all down
	defer func() {
		cancel()

		if err := errg.Wait().ErrorOrNil(); err != nil {
			t.Errorf("Error running processors: %v", err)
		}

		if err := sched.Close(); err != nil {
			t.Errorf("Error closing scheduler: %v", err)
		}
	}()

	client := NewOrderClient("")

	expectTarget := func(key, value string) {
		k, v, ok := targetTracker.Next()
		if !ok {
			t.Errorf("order not executed: no message in target topic")
			t.FailNow()
		}
		if k != key {
			t.Errorf("unexpected order key, was %s", k)
			t.FailNow()
		}
		if string(v.([]byte)) != value {
			t.Errorf("unexpected order value, was %s", v)
			t.FailNow()
		}
	}

	expectNoEmit := func() {
		if targetTracker.NextOffset() < targetTracker.Hwm() {
			t.Errorf("Expected no new emit, but got at least one")
		}
	}

	for _, test := range []struct {
		name   string
		orders []*testOrder
		expect func()
	}{
		{
			name: "single-delay",
			orders: []*testOrder{
				mustTestOrder(client.NewOrder([]byte("1"), "target", "1", []byte("42"), OrderType_Delay, 25*time.Second)),
			},
			expect: func() {
				expectTarget("1", "42")
				expectNoEmit()
			},
		},
		{
			name: "multi-delay",
			orders: []*testOrder{
				mustTestOrder(client.NewOrder([]byte("1"), "target", "1", []byte("42"), OrderType_Delay, 25*time.Second)),
			},
			expect: func() {
				expectTarget("1", "42")
				expectNoEmit()
			},
		},
		{
			name: "throttle",
			orders: []*testOrder{
				mustTestOrder(client.NewOrder([]byte("1"), "target", "", []byte("42"), OrderType_ThrottleFirst, 5*time.Second)),
			},
			expect: func() {
				expectTarget("1", "42")
				expectNoEmit()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			targetTracker.Seek(targetTracker.Hwm())
			gkt.ClearValues()

			done := make(chan struct{})
			go func() {
				for {
					select {
					case <-done:
						return
					default:
					}
					mockClk.Add(time.Second)
				}
			}()

			for _, order := range test.orders {
				gkt.Consume(string(client.OrderTopic()), order.key, order.order)
			}
			close(done)
			test.expect()
		})
	}
}

func ExampleScheduler() {

	// create new scheduler config
	cfg := NewConfig()
	// e.g. replace selected metrics to be exported
	cfg.WithMxZombieEvicted(func(value float64) {
		// handle metric
	})

	sched := CreateScheduler(cfg,
		[]time.Duration{
			1 * time.Hour,
			30 * time.Minute,
			1 * time.Minute,
			10 * time.Second,
			3 * time.Second,
		},
		func(topic goka.Stream, codec goka.Codec) (Emitter, error) {
			return goka.NewEmitter([]string{"localhost:9092"},
				topic,
				codec)
		},
		"my-scheduler", // some prefix if we have multiple schedulers running in the same kafka cluster
	)

	errg, ctx := multierr.NewErrGroup(context.Background())

	for _, graph := range sched.CreateGraphs() {
		proc, err := goka.NewProcessor([]string{"localhost:9092"}, graph)
		if err != nil {
			log.Fatalf("Error creating processor for graph %#v: %v", graph, err)
		}
		errg.Go(func() error {
			return proc.Run(ctx)
		})
	}

	if err := errg.Wait().ErrorOrNil(); err != nil {
		log.Printf("Error running scheduler: %v", err)
	}

	if err := sched.Close(); err != nil {
		log.Printf("Error closing scheduler: %v", err)
	}
}

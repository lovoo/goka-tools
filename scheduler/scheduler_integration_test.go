package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"os"
	"os/signal"
	sync "sync"
	"syscall"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

var (
	integrationTest = flag.Bool("integration-test", false, "set to run integration tests that require a running kafka")
	broker          = flag.String("broker", "localhost:9092", "kafka broker to use for bootstrapping. defaults to localhost:9092")
)

const (
	targetTopic = "sched-integration-test-target"
	schedPrefix = "goka-tools-scheduler-integration-tets"
)

func TestScheduler_Integration(t *testing.T) {
	if !*integrationTest {
		t.Skipf("Ignoring integration test. pass '-args -integration-test' to `go test` to run them")
	}

	// set some global config so we have more reliable results
	cfg := goka.DefaultConfig()
	// set it to oldest so we don't miss the first message on the first test in case the
	// scheduler processors are slow starting up
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.MaxWaitTime = 50 * time.Millisecond
	cfg.Producer.Flush.Frequency = 50 * time.Millisecond
	// flush every message immediately
	cfg.Producer.Flush.MaxMessages = 1
	goka.ReplaceGlobalConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	// make the test stoppable via ctrl-c
	errg.Go(func() error {
		waiter := make(chan os.Signal, 1)
		signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			return nil
		case <-waiter:
		}
		cancel()
		return nil
	})

	// run the scheduler
	startScheduler(ctx, errg, schedPrefix)
	receiver := createReceiverProc(ctx, t, errg)
	codec := new(codec.String)

	mustEncode := func(val interface{}) []byte {
		enc, err := codec.Encode(val)
		if err != nil {
			t.Errorf("Error encoding: %v", err)
		}
		return enc
	}

	orderClient := NewOrderClient(schedPrefix)
	orderEmitter, err := goka.NewEmitter([]string{*broker}, orderClient.OrderTopic(), orderClient.OrderEdge().Codec())
	if err != nil {
		t.Errorf("Error creating order emitter: %v", err)
	}

	log.Printf("waiting for processors to get up")
	// need some time to start the processors
	time.Sleep(10 * time.Second)

	// no need to finish the emitter after the unit test.
	emitThrottleFirst := func(key, value string, delay time.Duration) {
		key, order, err := orderClient.NewOrder([]byte(key), targetTopic, "", mustEncode(value), OrderType_ThrottleFirst, delay)
		if err != nil {
			t.Errorf("error creating order: %v", err)
		}
		err = orderEmitter.EmitSync(key, order)
		if err != nil {
			t.Errorf("Error emitting: %v", err)
		}
	}
	emitThrottleFirstReschedule := func(key, value string, delay time.Duration) {
		key, order, err := orderClient.NewOrder([]byte(key), targetTopic, "", mustEncode(value), OrderType_ThrottleFirstReschedule, delay)
		if err != nil {
			t.Errorf("error creating order: %v", err)
		}
		err = orderEmitter.EmitSync(key, order)
		if err != nil {
			t.Errorf("Error emitting: %v", err)
		}
	}

	// delay is so slow that the scheduler re-emits back immediately
	t.Run("throttle-immediate", func(t *testing.T) {
		key := randKey()
		emitThrottleFirst(key, "immediate", 1*time.Millisecond)
		receiver.checkValue(t, 10*time.Second, key, "immediate")
		receiver.clear()
	})

	t.Run("throttle-first", func(t *testing.T) {
		key := randKey()
		emitThrottleFirst(key, "value1", 120*time.Millisecond)
		// will be dropped
		emitThrottleFirst(key, "value2", 121*time.Millisecond)
		emitThrottleFirst(key, "value3", 122*time.Millisecond)
		receiver.checkValue(t, 10*time.Second, key, "value1")
		receiver.clear()
	})
	t.Run("throttle-first-reschedule", func(t *testing.T) {
		key := randKey()
		emitThrottleFirstReschedule(key, "value1", 10*time.Second)
		// will be dropped
		emitThrottleFirstReschedule(key, "value2", 120*time.Millisecond)
		emitThrottleFirstReschedule(key, "value3", 400*time.Millisecond)
		receiver.checkValue(t, 10*time.Second, key, "value2")
		receiver.clear()
	})

	cancel()

	defer errg.Wait()
	if err := errg.Wait().NilOrError(); err != nil {
		t.Errorf("Error running scheduler: %v", err)
	}
}

func startScheduler(ctx context.Context, errg *multierr.ErrGroup, prefix string) *Scheduler {

	sched := CreateScheduler(NewConfig(), []time.Duration{
		1 * time.Second,
		50 * time.Millisecond,
	}, func(stream goka.Stream, codec goka.Codec) (Emitter, error) {
		return goka.NewEmitter([]string{*broker}, stream, codec)
	}, prefix)

	var procs []*goka.Processor

	for _, graph := range sched.CreateGraphs() {
		waitProc, err := goka.NewProcessor([]string{*broker},
			graph,
			goka.WithStorageBuilder(storage.MemoryBuilder()))
		if err != nil {
			log.Fatalf("Error creating wait processor for graph %v: %v", graph, err)
		}
		procs = append(procs, waitProc)
	}

	for _, proc := range procs {
		proc := proc
		errg.Go(func() error {
			return proc.Run(ctx)
		})
	}
	return sched
}

type receiverProc struct {
	m        sync.Mutex
	received map[string]string
}

func createReceiverProc(ctx context.Context, t *testing.T, errg *multierr.ErrGroup) *receiverProc {

	rp := &receiverProc{
		received: make(map[string]string),
	}
	proc, err := goka.NewProcessor([]string{*broker}, goka.DefineGroup(
		"receiver-proc",
		goka.Input(targetTopic, new(codec.String), func(ctx goka.Context, msg interface{}) {
			// log.Printf("receiving %s, %v", ctx.Key(), msg)
			rp.m.Lock()
			defer rp.m.Unlock()
			rp.received[ctx.Key()] = msg.(string)
		}),
	),
		goka.WithStorageBuilder(storage.MemoryBuilder()),
	)
	if err != nil {
		t.Errorf("error creating receiver processor: %v", err)
	}

	errg.Go(func() error {
		return proc.Run(ctx)
	})

	return rp
}
func (rp *receiverProc) checkValue(t *testing.T, timeout time.Duration, key string, value string) {

	var (
		waitTime = 10 * time.Millisecond
	)
	start := time.Now()
	for {
		time.Sleep(waitTime)

		rp.m.Lock()
		val := rp.received[key]
		rp.m.Unlock()
		if val == value {
			return
		}

		if time.Since(start) > timeout {
			t.Errorf("timed out waiting for %s==%s. last value was %s", key, value, val)
			return
		}
	}
}

func (rp *receiverProc) clear() {
	rp.m.Lock()
	defer rp.m.Unlock()

	rp.received = make(map[string]string)
}

func randKey() string {
	var target = make([]byte, 4)
	rand.Read(target)

	return hex.EncodeToString(target)
}

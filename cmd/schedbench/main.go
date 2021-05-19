package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka-tools/scheduler"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/spf13/pflag"
)

var (
	brokers = pflag.StringSlice("brokers", []string{"localhost:9092"}, "brokers to connect to")
)

func main() {

	pflag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	errg.Go(func() error {
		waiter := make(chan os.Signal, 1)
		signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)
		<-waiter
		cancel()
		return nil
	})

	startScheduler(ctx, errg)

	startExperiment(ctx, errg)

	if err := errg.Wait().ErrorOrNil(); err != nil {
		log.Fatalf("Error running processors")
	}

	cleanEmitters()

}

func startExperiment(ctx context.Context, errg *multierr.ErrGroup) {

	orderClient := scheduler.NewOrderClient("")

	emitter, err := goka.NewEmitter(*brokers, orderClient.OrderTopic(), scheduler.NewOrderCodec())

	if err != nil {
		log.Fatalf("Error starting order emitter: %v", err)
	}

	testProc, err := goka.NewProcessor(*brokers, goka.DefineGroup("scheduler-test",
		goka.Input("target", new(codec.Int64), func(ctx goka.Context, msg interface{}) {
			targetTime := msg.(int64)

			log.Printf("Target execution time off by %d ms", (time.Now().UnixNano()-targetTime)/1000000)
		}),
	),
	)

	if err != nil {
		log.Fatalf("Error starting processor: %v", err)
	}

	errg.Go(func() error {
		return testProc.Run(ctx)
	})

	intCodec := new(codec.Int64)

	for i := 0; i <= 1000; i++ {

		delay := rand.Int63n(60) + 30

		// time when it should get executed
		targetNano := time.Now().UnixNano() + delay*int64(time.Second)
		// we'll encode that in the message so the processor can compare if it was actually executed at that time
		msg, _ := intCodec.Encode(targetNano)
		key, order, err := orderClient.NewOrder([]byte(fmt.Sprintf("%d", i)),
			"target", "", msg, scheduler.OrderType_ThrottleFirst, time.Duration(delay)*time.Second)
		if err != nil {
			log.Fatalf("Error creating order: %v", err)
		}

		prom, err := emitter.Emit(key, order)
		if err != nil {
			log.Fatalf("Error emitting: %v", err)
		}
		prom.Then(func(err error) {
			if err != nil {
				log.Printf("Error emitting: %v", err)
			}
		})

		select {
		case <-ctx.Done():
			break
		default:
		}

		time.Sleep(50 * time.Millisecond)
	}

	err = emitter.Finish()
	if err != nil {
		log.Fatalf("error finishing emitter: %v", err)
	} else {
		log.Printf("emitter finished cleanly")
	}
}

var (
	mEmitters sync.Mutex
	emitters  = make(map[string]*goka.Emitter)
)

func emitterFactory(stream goka.Stream, codec goka.Codec) (scheduler.Emitter, error) {
	mEmitters.Lock()
	defer mEmitters.Unlock()

	if emitter, ok := emitters[string(stream)]; ok {
		return emitter, nil
	}

	emitter, err := goka.NewEmitter(*brokers, stream, codec)
	if err == nil {
		emitters[string(stream)] = emitter
	}
	return emitter, err
}

func cleanEmitters() {
	mEmitters.Lock()
	defer mEmitters.Unlock()

	var (
		wg   sync.WaitGroup
		errs = new(multierr.Errors)
	)

	for _, emitter := range emitters {
		wg.Add(1)
		emitter := emitter
		go func() {
			defer wg.Done()
			errs.Collect(emitter.Finish())
		}()
	}
	wg.Wait()

	if err := errs.NilOrError(); err != nil {
		log.Printf("Error flushing scheduler emitters: %v", err)
	}
}

func startScheduler(ctx context.Context, errg *multierr.ErrGroup) {

	sched := scheduler.CreateScheduler(scheduler.NewConfig(), []time.Duration{
		1 * time.Hour,
		30 * time.Minute,
		1 * time.Minute,
		10 * time.Second,
		3 * time.Second,
		1 * time.Second,
		300 * time.Millisecond,
	}, func(stream goka.Stream, codec goka.Codec) (scheduler.Emitter, error) {
		return goka.NewEmitter(*brokers, stream, codec)
	}, "")

	var procs []*goka.Processor

	for _, graph := range sched.CreateGraphs() {
		waitProc, err := goka.NewProcessor(*brokers, graph)
		if err != nil {
			log.Fatalf("Error creating wait processor for graph %v: %v", graph, err)
		}
		procs = append(procs, waitProc)
	}

	for _, proc := range procs {
		proc := proc
		errg.Go(func() error {
			err := proc.Run(ctx)

			if err != nil {
				log.Printf("Error running processor %v", err)
			} else {
				log.Printf("processor shutdown cleanly")
			}
			return err
		})
	}

}

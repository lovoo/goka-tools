package disktail_test

import (
	"context"

	"github.com/lovoo/goka-tools/disktail"
	"github.com/lovoo/goka-tools/gtyped"
)

func ExampleDiskTail() {
	tail, err := disktail.NewDiskTail([]string{"broker:9092"},
		"topic",
		new(gtyped.StringCodec[string]),
		"/tmp/disktaildata",
		&disktail.Config{})

	if err != nil {
		// handle error
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	done := make(chan struct{})

	go func() {
		defer close(done)
		err := tail.Run(ctx)

		if err != nil {
			// handle error
		}
	}()

	stats := &disktail.IterStats{}
	tail.Iterate(ctx, true, stats, func(item *disktail.Item[string]) disktail.HandleResult {
		// handle the iterated item

		return disktail.Continue
	})

	cancel()
	<-done

}

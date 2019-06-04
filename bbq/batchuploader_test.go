package bbq

import (
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/facebookgo/ensure"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

// our fake uploader that just counts how many items have been put in there
// and how many times it's been called
type testUploader struct {
	calls int
	items int
}

// mocks the real function of bigquery's uploader to put a row (or rows) into
// a table. Here we just count it and the items.
func (tu *testUploader) Put(ctx context.Context, src interface{}) error {
	tu.calls++
	items, ok := src.([]bigquery.ValueSaver)
	if ok {
		tu.items += len(items)
	}
	return nil
}

// reset the counters for a test
func (tu *testUploader) reset() {
	tu.calls = 0
	tu.items = 0
}

func TestBatchUploader(t *testing.T) {

	// create everything we need for the uploader
	stop := make(chan bool, 0)
	var wg sync.WaitGroup
	up := new(testUploader)
	uploadedCounter := prometheus.NewCounter(prometheus.CounterOpts{})
	uploadFailedCounter := prometheus.NewCounter(prometheus.CounterOpts{})

	// create an uploaer with batchsize 10 and a 100ms timeout
	uploader := newBatchedUploader(stop, &wg, "test", up,
		uploadedCounter, uploadFailedCounter,
		10, 100*time.Millisecond)

	// add less than a batch and wait for the timeout
	t.Run("timeout", func(t *testing.T) {
		up.reset()
		for i := 0; i < 9; i++ {
			uploader.Upload(new(saver))
		}
		// nothing uploaded yet
		ensure.DeepEqual(t, up.calls, 0)
		ensure.DeepEqual(t, up.items, 0)

		// wait until timeout
		time.Sleep(150 * time.Millisecond)
		ensure.DeepEqual(t, up.calls, 1)
		ensure.DeepEqual(t, up.items, 9)

	})

	// add several batches
	t.Run("batch full", func(t *testing.T) {
		up.reset()
		for i := 0; i < 21; i++ {
			uploader.Upload(new(saver))
		}
		// wait some time so the uploader can catch up
		time.Sleep(10 * time.Millisecond)
		// at least two batches are full.
		// if it timed out in between there may be 3
		ensure.True(t, up.calls >= 2, up.calls)

		// wait until timeout, now we should have 3
		time.Sleep(150 * time.Millisecond)
		ensure.DeepEqual(t, up.calls, 3)
		ensure.DeepEqual(t, up.items, 21)
	})

	// add many items and test the graceful draining of the input channel.
	t.Run("stopping", func(t *testing.T) {
		up.reset()
		// add 1000 asynchronusly
		go func() {
			for i := 0; i < 1000; i++ {
				uploader.Upload(new(saver))
			}
			// stop it right away
			close(stop)
		}()
		wg.Wait()

		// we should still have all the calls (100 * batchsize = 1000)
		ensure.DeepEqual(t, up.calls, 100)
		ensure.DeepEqual(t, up.items, 1000)
	})

}

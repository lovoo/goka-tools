package bbq

import (
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"

	"cloud.google.com/go/bigquery"
)

type batchedUploader struct {
	table        string
	uploader     uploader
	uploaded     prometheus.Counter
	uploadFailed prometheus.Counter
	vals         chan bigquery.ValueSaver
	customObject func(interface{}) interface{}
}

// helper interface that abstracts bigquery's table uploader so we can
// just replace it with a mock in the unit test
type uploader interface {
	Put(context.Context, interface{}) error
}

func newBatchedUploader(stop chan bool, wg *sync.WaitGroup, table string, uploader uploader, uploaded prometheus.Counter, uploadFailed prometheus.Counter, batchSize int,
	timeout time.Duration, customObject func(interface{}) interface{}) *batchedUploader {
	bu := &batchedUploader{
		table:        table,
		uploader:     uploader,
		uploaded:     uploaded,
		uploadFailed: uploadFailed,
		vals:         make(chan bigquery.ValueSaver, batchSize),
		customObject: customObject,
	}

	go func() {
		// wait for the stop-channel to close, then close our vals-channel so we can drain it.
		<-stop
		close(bu.vals)
	}()

	wg.Add(1)
	bu.start(wg, batchSize, timeout)
	return bu
}

func (bu *batchedUploader) start(wg *sync.WaitGroup, batchSize int, timeout time.Duration) {
	ctx := context.Background()

	// holds the values in the current batch
	batch := make([]bigquery.ValueSaver, 0, batchSize)

	// local function to send the batch
	sendBatch := func() {
		if len(batch) == 0 {
			return
		}

		// add a timeout to our context for the call
		timeoutCtx, cancel := context.WithTimeout(ctx, bigqueryUploadTimeout)
		defer cancel()
		err := bu.uploader.Put(timeoutCtx, batch)

		// log the error and increase a metric but do not abort.
		if err != nil {
			multiError, ok := err.(bigquery.PutMultiError)
			if ok {
				for _, multiErr := range multiError {
					log.Printf("Error uploading message from topic %s to BQ: %v", bu.table, multiErr)
				}
				bu.uploadFailed.Add(float64(len(multiError)))
			} else {
				log.Printf("Error uploading message from topic %s to BQ: %v. MultiError cast failed", bu.table, err)
				bu.uploadFailed.Inc()
			}
		} else {
			bu.uploaded.Add(float64(len(batch)))
		}
		batch = make([]bigquery.ValueSaver, 0, batchSize)
	}

	// a ticker for sending batches even if the batch size is not full.
	batchTimeoutTicker := time.NewTicker(timeout)

	go func() {
		defer wg.Done()
		defer batchTimeoutTicker.Stop()
		// send a last batch at the end, if there's still something left
		defer sendBatch()

		for {
			select {
			// if there is a new value in our incoming values-channel
			case val, ok := <-bu.vals:
				// channel closed, stop here
				if !ok {
					return
				}
				// ... or add it to the batch and send the batch if full
				batch = append(batch, val)
				if len(batch) >= batchSize {
					sendBatch()
				}

				// regularily send the batch even if it's not full.
			case <-batchTimeoutTicker.C:
				sendBatch()
			}
		}
	}()

}

// Upload adds a new value to the uploader which sends multiple values
// as batches
func (bu *batchedUploader) Upload(val bigquery.ValueSaver) {
	bu.vals <- val
}

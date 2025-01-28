package disktail

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/benbjohnson/clock"
	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka-tools/gtyped"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

const (
	iterationChannelSize = 100
)

var nosyncOption = &pebble.WriteOptions{
	Sync: false,
}

type Logger interface {
	Info(format string, args ...interface{})
}

type nilLogger struct{}

func (n *nilLogger) Info(format string, args ...interface{}) {}

var offsetWriteRate int64 = 1000

// DiskTail implements a persistent tail for a kafka topic.
// Data is stored to disk using pebble key-value store.
// It stores the received messages using their message-timestamp so they can be
// iterated in a timely manner,
// optionally skipping duplicate keys.
//
// It uses two different types of keys:
//
//	offset-keys: the key to represent a partition offset is merely the partition number,
//	encoded as byte slice. 0 == [00 00 00 00], 1 == [00 00 00 01] etc...
//
//	data-keys: the key to represent a data message. This key contains
//	   * 8 bytes timestamp in microseconds
//	   * 8 bytes offset
//	   * 4 bytes partition
//
// That way we keep the keys unique across partitions, even if they have the same timestamp but
// still maintain the order of their appearance in the topic.
//
// So the storage keys will look like this (in order):
// [00 00 00 00]  13  <- offset for partition 0 is 13
// [00 00 00 01]  42  <- offset for partition 1 is 42
// ...
// [00 06 18 1d 49 f2 53 80 00 00 00 00 00 01 ...] // data keys
//
// Cleaning up deletes the range starting from [ff ff ff ff] to the oldest time range
type DiskTail[T any] struct {
	codec gtyped.GCodec[T]

	config *Config

	topic string

	store *pebble.DB

	consumer sarama.Consumer

	messages chan *sarama.ConsumerMessage

	// metrics
	mxAdded   prometheus.Counter
	mxTimeLag *prometheus.GaugeVec
	mxCleaned prometheus.Counter
}

type Config struct {
	ConsumerBuilder goka.SaramaConsumerBuilder
	MaxAge          time.Duration
	CleanInterval   time.Duration
	Clk             clock.Clock
	InitialOffset   int64
	ClientId        string // client-id used for sarama and for the goka tester
	Log             Logger
}

func (c *Config) defaults() *Config {
	if c == nil {
		c = &Config{}
	}

	// use goka's default consumer builder
	if c.ConsumerBuilder == nil {
		c.ConsumerBuilder = goka.SaramaConsumerBuilderWithConfig(goka.DefaultConfig())
	}

	// 7 days is default
	if c.MaxAge <= 0 {
		c.MaxAge = 7 * 24 * time.Hour
	}

	// clean every hour
	if c.CleanInterval <= 0 {
		c.CleanInterval = 1 * time.Hour
	}

	// use the default clock
	if c.Clk == nil {
		c.Clk = clock.New()
	}

	// TODO: implement auto-deduplicator, add that to config
	// iterate doesn't have to have the dedup option

	if c.ClientId == "" {
		c.ClientId = "disktail"
	}

	if c.Log == nil {
		c.Log = new(nilLogger)
	}
	return c
}

// Marks the end of the offset-key-range, i.e. all offset keys will
// be smaller and all data-keys will be bigger.
// this is used as a range-start to delete old values in the tailer
var endOffsetRange = encodeOffsetKey(math.MaxInt32)

func NewDiskTail[T any](brokers []string, topic string, codec gtyped.GCodec[T], path string, config *Config) (*DiskTail[T], error) {

	config = config.defaults()

	consumer, err := config.ConsumerBuilder(brokers, config.ClientId)

	if err != nil {
		return nil, fmt.Errorf("error building consumer: %w", err)
	}

	cache := pebble.NewCache(500 << 20) // << 20 shifts to MB

	store, err := pebble.Open(path, &pebble.Options{
		DisableWAL:                  true,
		Cache:                       cache,
		DisableAutomaticCompactions: false,
		TableCache:                  pebble.NewTableCache(cache, 20, 100),
		MemTableSize:                uint64(100 << 20), // 100mb memtable to avoid too many compactions
		MaxConcurrentCompactions:    func() int { return 3 },
	})
	if err != nil {
		return nil, fmt.Errorf("error opening storage %s: %w", path, err)
	}

	ring := &DiskTail[T]{
		config:   config,
		consumer: consumer,
		messages: make(chan *sarama.ConsumerMessage, 1000),
		topic:    topic,
		store:    store,
		codec:    codec,

		mxAdded: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "goka_tools",
			Subsystem: "disktail",
			Name:      "added_item",
			Help:      "items added to the tail",
		}),
		mxTimeLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "goka_tools",
			Subsystem: "disktail",
			Name:      "timestamp_lag_s",
			Help:      "timestamp milliseconds of last added message",
		}, []string{"partition"}),
		mxCleaned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "goka_tools",
			Subsystem: "disktail",
			Name:      "cleaned",
			Help:      "a cleaning job was executed",
		}),
	}

	return ring, nil
}

func (r *DiskTail[T]) Metrics() []prometheus.Collector {
	return []prometheus.Collector{
		r.mxAdded,
		r.mxTimeLag,
		r.mxCleaned,
	}
}

func (r *DiskTail[T]) close() error {
	return multierror.Append(
		r.store.Close(),
		r.consumer.Close(),
	).ErrorOrNil()
}

// Run starts the disk tailer. It's only allowed to be called once per instance.
func (r *DiskTail[T]) Run(ctx context.Context) error {
	defer r.close()

	parts, err := r.consumer.Partitions(r.topic)
	if err != nil {
		return fmt.Errorf("error getting partitions for topic %s: %w", r.topic, err)
	}

	storedOffsets, err := r.getOffsets(parts)
	if err != nil {
		return fmt.Errorf("error reading local offsets: %w", err)
	}

	errg, ctx := errgroup.WithContext(ctx)
	for _, part := range parts {
		part := part
		errg.Go(func() error {
			r.config.Log.Info("creating worker for partition %d", part)
			offset := storedOffsets[part]
			if offset == -1 {
				offset = r.config.InitialOffset
			} else {
				// request the "next" offset
				offset++
			}

			r.config.Log.Info("starting to consume partition %d from offset %d", part, offset)

			partCons, err := r.consumer.ConsumePartition(r.topic, part, offset)

			// if out of range, try with oldest
			if err == sarama.ErrOffsetOutOfRange {
				r.config.Log.Info("consumer offset is out of range, will try with 'oldest'")
				partCons, err = r.consumer.ConsumePartition(r.topic, part, sarama.OffsetOldest)
			}

			if err != nil {
				return fmt.Errorf("error consuming partition %s/%d: %w", r.topic, part, err)
			}

			msgs := partCons.Messages()
			for {
				select {
				case <-ctx.Done():
					return partCons.Close()
				case msg, ok := <-msgs:
					if !ok {
						return nil
					}
					select {
					case r.messages <- msg:
					case <-ctx.Done():
						return partCons.Close()
					}
				}
			}
		})
	}

	errg.Go(func() error {
		return r.handleMessages(ctx)
	})

	errg.Go(func() error {
		return r.cleaner(ctx)
	})

	return errg.Wait()

}

// handle messages from kafka.
func (r *DiskTail[T]) handleMessages(ctx context.Context) error {
	for {
		select {
		case msg, ok := <-r.messages:
			if !ok {
				// channel closed, return
				return nil
			}

			// Those nil-values will never be send by kafka/sarama, but injected by the goka-tester to achieve synchronous
			// testing behavior. To support the tester, we drop the messages here, as expected.
			if msg == nil {
				continue
			}

			ts := msg.Timestamp
			if ts.IsZero() {
				r.config.Log.Info("no timestamp is zero, will assume 'now'")
				ts = r.config.Clk.Now()
			}
			key := encodeKey(ts, msg.Offset, msg.Partition)
			err := r.store.Set(key, encodeValue(msg.Key, msg.Value), nosyncOption)
			if err != nil {
				return fmt.Errorf("error storing value for key %s (partition=%d): %w", string(msg.Key), msg.Partition, err)
			}

			r.mxAdded.Add(1)
			r.mxTimeLag.WithLabelValues(strconv.Itoa(int(msg.Partition))).Set(time.Since(ts).Seconds())

			// let's store offset only every 1k message, it's fine to do some catchup on the last message
			if msg.Offset%offsetWriteRate == 0 {
				if err := r.storeOffset(msg.Partition, msg.Offset); err != nil {
					return fmt.Errorf("error storing offset %d for partition %d: %w", msg.Offset, msg.Partition, err)
				}
			}
		case <-ctx.Done():
			// context closed, just stop.
			// we don't care about messages in channel, no need to drain or close it as they will be re-consumed
			return nil
		}
	}
}

func (r *DiskTail[T]) storeOffset(partition int32, offset int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	return r.store.Set(encodeOffsetKey(partition), buf, nosyncOption)
}

func (r *DiskTail[T]) getOffsets(partitions []int32) ([]int64, error) {
	offsets := make([]int64, len(partitions))

	for idx, part := range partitions {
		val, closer, err := r.store.Get(encodeOffsetKey(part))
		if len(val) != 8 {
			r.config.Log.Info("offset value has unexpected length of %d (%x)", len(val), val)
			offsets[idx] = -1
			continue
		}
		if err != nil {
			if err == pebble.ErrNotFound {
				offsets[idx] = -1
				continue
			}
			return nil, fmt.Errorf("error reading offset for partition %d: %w", part, err)
		}
		offsets[idx] = int64(binary.BigEndian.Uint64(val))
		closer.Close()
	}
	return offsets, nil
}

func (r *DiskTail[T]) cleaner(ctx context.Context) error {

	ticker := r.config.Clk.Ticker(r.config.CleanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// oldest timestamp based on now - max age
			oldest := r.config.Clk.Now().Add(-r.config.MaxAge)

			r.config.Log.Info("cleaning disk tail, removing entries older than %v", oldest)
			// delete the whole range, starting from the minimal offsetkey
			if err := r.store.DeleteRange(endOffsetRange, encodeKey(oldest, 0, 0), pebble.NoSync); err != nil {
				r.config.Log.Info("error deleting range: %v", err)
			}
			r.mxCleaned.Add(1)
		}
	}
}

type IterStats struct {
	m sync.Mutex
	// number of items iterated
	itemsIterated atomic.Int64
	// number of unique keys. Note that this value will be 0 if duplicates are included when
	// iterating.
	uniqueKeys atomic.Int64

	// time of the oldest item being iterated
	oldestItem time.Time
}

func (is *IterStats) ItemsIterated() int64 {
	return is.itemsIterated.Load()
}

func (is *IterStats) UniqueKeys() int64 {
	return is.uniqueKeys.Load()
}

func (is *IterStats) OldestItem() time.Time {
	is.m.Lock()
	defer is.m.Unlock()

	return is.oldestItem
}

type Item[T any] struct {
	value     []byte
	Key       string
	Timestamp time.Time
	Partition int32
	codec     gtyped.GCodec[T]
}

func (i *Item[T]) Value() (T, error) {
	item, err := i.codec.GDecode(i.value)
	if err != nil {
		return item, fmt.Errorf("error decoding value: %w", err)
	}
	return item, nil
}

type HandleResult int32

const (
	Continue HandleResult = 0
	Stop     HandleResult = 1
)

func (r *DiskTail[T]) Iterate(ctx context.Context, skipDuplicates bool, stats *IterStats, handler func(item *Item[T]) HandleResult) error {
	if stats == nil {
		stats = &IterStats{}
	}

	it, err := r.store.NewIter(nil)
	if err != nil {
		return fmt.Errorf("error creating iterator: %w", err)
	}
	r.store.NewBatch()
	if !it.Last() {
		return nil
	}

	itemHandler := handler

	if skipDuplicates {

		keys := struct {
			sync.Mutex
			keys map[string]struct{}
		}{keys: make(map[string]struct{})}

		itemHandler = func(item *Item[T]) HandleResult {

			keys.Lock()
			if _, ok := keys.keys[item.Key]; ok {
				// skip it
				keys.Unlock()
				return Continue
			}

			keys.keys[item.Key] = struct{}{}
			keys.Unlock()

			stats.uniqueKeys.Add(1)

			// call original handler
			return handler(item)
		}
	}

	// create workers for each partition
	workers := newPartitionWorkers[*Item[T]](ctx, 20)
	defer workers.stop()

iterateTail:
	for ctx.Err() == nil {

		key := it.Key()

		// track oldest timestamp for stats
		timestamp, _, partition := decodeKey(key)

		// not a valid data-key (probably offset key), skip it
		if timestamp.UnixMicro() == 0 {
			if it.Prev() {
				continue
			}
			// we're at the beginning, stopping loop
			break
		}

		stats.m.Lock()
		if stats.oldestItem.IsZero() || timestamp.Before(stats.oldestItem) {
			stats.oldestItem = timestamp
		}
		stats.m.Unlock()

		// decode the stored value into key-value, so we can
		// decode it using the codec and deduplicate using the key.
		key, value := decodeValue(it.Value())

		stats.itemsIterated.Add(1)

		workerChan := workers.getWorker(partition, itemHandler)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value) // decoding is more expensive than copying the slice, so let's try that
		select {
		case workerChan <- &Item[T]{
			value:     valueCopy,
			Key:       string(key),
			Timestamp: timestamp,
			Partition: partition,
			codec:     r.codec,
		}:
		case <-ctx.Done():
			break iterateTail

			// if the handlers are stopping no point in continuing to iterate
		case <-workers.Done():
			break iterateTail
		}

		if !it.Prev() {
			break
		}
	}

	// done iterating, close all worker channels and wait for the workers
	return workers.closeAndWait()
}

// to keep message order intact regarding its original partition, we'll
// create as many workers as there are partitions (lazily).
// Then each messages gets send to its original partition to be handled.
type partitionWorkers[T any] struct {
	m      sync.RWMutex
	chans  map[int32]chan T
	errg   *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc
}

func newPartitionWorkers[T any](ctx context.Context, initialPartitions int32) *partitionWorkers[T] {

	workerCtx, cancel := context.WithCancel(ctx)

	workerGroup, workerCtx := errgroup.WithContext(workerCtx)

	return &partitionWorkers[T]{
		chans:  make(map[int32]chan T, initialPartitions),
		errg:   workerGroup,
		ctx:    workerCtx,
		cancel: cancel,
	}
}

func (pw *partitionWorkers[T]) getWorker(partition int32, itemHandler func(T) HandleResult) chan T {

	var (
		c  chan T
		ok bool
	)

	pw.m.RLock()
	c, ok = pw.chans[partition]
	if ok {
		pw.m.RUnlock()
		return c
	}
	pw.m.RUnlock()
	pw.m.Lock()
	c, ok = pw.chans[partition]
	if ok {
		pw.m.Unlock()
		return c
	}

	defer pw.m.Unlock()

	c = make(chan T, iterationChannelSize)

	pw.errg.Go(func() error {
		for {

			select {
			case item, ok := <-c:
				if !ok {
					return nil
				}
				// if we should stop, we stop
				if itemHandler(item) == Stop {
					pw.stop()
					return nil
				}
			case <-pw.ctx.Done():
				return nil
			}
		}
	})

	pw.chans[partition] = c

	return c
}
func (pw *partitionWorkers[T]) closeAndWait() error {
	for _, worker := range pw.chans {
		close(worker)
	}
	return pw.errg.Wait()
}

func (pw *partitionWorkers[T]) stop() {
	pw.cancel()
}

func (pw *partitionWorkers[T]) Done() <-chan struct{} {
	return pw.ctx.Done()
}

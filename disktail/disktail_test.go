package disktail

import (
	"bytes"
	"context"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/benbjohnson/clock"
	"github.com/lovoo/goka-tools/gtyped"
	"github.com/stretchr/testify/require"
)

// Tests encoding the key based on timestamp-offset-partition for storage
func TestEncodeDecodeKey(t *testing.T) {

	for _, test := range []struct {
		name      string
		timestamp time.Time
		partition int32
		offset    int64
	}{
		{name: "empty"},
		{
			name:      "1",
			timestamp: time.Unix(1715363936, 0),
			partition: 23,
			offset:    7,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded := encodeKey(test.timestamp, test.offset, test.partition)

			require.Len(t, encoded, keyLen)

			decTimestamp, decOffset, decPartition := decodeKey(encoded)
			require.EqualValues(t, test.timestamp.UTC(), decTimestamp.UTC())
			require.EqualValues(t, test.offset, decOffset)
			require.EqualValues(t, test.partition, decPartition)
		})
	}

}

// Tests to encode/decode the key/value of the message into the value of the
// storage
func TestEncodeDecodeValue(t *testing.T) {

	for _, test := range []struct {
		name  string
		key   []byte
		value []byte
	}{
		{name: "empty",
			key:   []byte{},
			value: []byte{},
		},
		{
			name:  "filled",
			key:   []byte{0x01, 0x02},
			value: []byte{0x03, 0x04},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded := encodeValue(test.key, test.value)

			require.Len(t, encoded, len(test.key)+len(test.value)+4)

			decodedKey, decodedValue := decodeValue(encoded)
			require.EqualValues(t, decodedKey, test.key)
			require.EqualValues(t, decodedValue, test.value)
		})

	}
}

// Tests that the order of keys is maintained based on time even across different
// partitions.
func TestOrder(t *testing.T) {

	now := time.Now()
	highestPartitionKey := encodeKey(now, 0, math.MaxInt32)
	require.EqualValues(t, -1, bytes.Compare(endOffsetRange, highestPartitionKey))

	lowestPartitionKey := encodeKey(now, 0, 0)
	require.EqualValuesf(t, -1, bytes.Compare(lowestPartitionKey, highestPartitionKey), "lowest %v, highest %v", lowestPartitionKey, highestPartitionKey)
	require.EqualValues(t, -1, bytes.Compare(endOffsetRange, highestPartitionKey))

	// data1 is before data2 because timestamp is lower, even though offset and partition is higher
	data1 := encodeKey(time.Unix(1715363934, 0), 2, 1)
	data2 := encodeKey(time.Unix(1715363935, 0), 1, 0)
	require.EqualValues(t, -1, bytes.Compare(data1, data2))
}

func TestDiskTail(t *testing.T) {

	now := time.Unix(1717138410, 0)
	clk := clock.NewMock()
	clk.Set(now)

	oldOffsetWriteRate := offsetWriteRate
	defer func() {
		offsetWriteRate = oldOffsetWriteRate
	}()

	offsetWriteRate = 1

	createTailer := func(t *testing.T, numPartitions int32, cleanerInterval time.Duration, cleanerMaxAge time.Duration) (*mocks.Consumer, *DiskTail[string]) {
		dir, err := os.MkdirTemp("", "disktail*")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		mockCons := mocks.NewConsumer(t, nil)

		var parts []int32
		for i := int32(0); i < numPartitions; i++ {
			parts = append(parts, i)
		}
		mockCons.SetTopicMetadata(map[string][]int32{"topic": parts})

		tail, err := NewDiskTail(nil, "topic", new(gtyped.StringCodec[string]), dir, &Config{
			ConsumerBuilder: func(brokers []string, client string) (sarama.Consumer, error) {
				return mockCons, nil
			},
			Clk:           clk,
			MaxAge:        cleanerMaxAge,
			CleanInterval: cleanerInterval,
			InitialOffset: sarama.OffsetNewest,
		})
		require.NoError(t, err)

		return mockCons, tail
	}

	msg := func(key string, value string, timeOffset int64) *sarama.ConsumerMessage {
		// create consumer-message, no need to set partition or offset, as both are set
		// in the mock.
		return &sarama.ConsumerMessage{
			Timestamp: clk.Now().Add(time.Duration(timeOffset) * time.Second),
			Key:       []byte(key),
			Value:     []byte(value),
		}
	}
	t.Run("empty-store-offset", func(t *testing.T) {
		mock, dt := createTailer(t, 2, 0, 0)

		p1 := mock.ExpectConsumePartition("topic", 0, sarama.OffsetNewest)
		p2 := mock.ExpectConsumePartition("topic", 1, sarama.OffsetNewest)

		// since the first message's offset is -1 (which seems like a bug in the mock)
		// we end up with the following message-offsets:
		// p0 -> -1, 0
		// p1 -> -1, 0, 1
		p1.YieldMessage(msg("a", "b", 1))
		p1.YieldMessage(msg("a", "b", 1))
		p2.YieldMessage(msg("c", "d", 2))
		p2.YieldMessage(msg("c", "d", 2))
		p2.YieldMessage(msg("c", "d", 2))

		ctx, cancel := context.WithCancel(context.Background())

		var (
			done   = make(chan struct{})
			runErr error
		)
		go func() {
			defer close(done)
			runErr = dt.Run(ctx)
		}()

		waitDelay(t, func() bool {
			offsets, err := dt.getOffsets([]int32{0, 1})
			require.NoError(t, err)
			return offsets[0] == 0 && offsets[1] == 1
		})

		cancel()
		<-done
		require.NoError(t, runErr)
	})

	t.Run("load-offsets", func(t *testing.T) {
		mock, dt := createTailer(t, 1, 0, 0)

		ctx, cancel := context.WithCancel(context.Background())

		var (
			done   = make(chan struct{})
			runErr error
		)

		// offset 3 is stored, so we'll expect offset 4 to be consumed
		require.NoError(t, dt.storeOffset(0, 3))
		p1 := mock.ExpectConsumePartition("topic", 0, 4)

		// emit some more messages
		p1.YieldMessage(msg("e", "f", 15)) // 4
		p1.YieldMessage(msg("e", "f", 15)) // 5
		p1.YieldMessage(msg("e", "f", 15)) // 6

		go func() {
			defer close(done)
			runErr = dt.Run(ctx)
		}()

		waitDelay(t, func() bool {
			offsets, err := dt.getOffsets([]int32{0})
			require.NoError(t, err)

			return offsets[0] == 6
		})

		cancel()
		<-done
		require.NoError(t, runErr)
	})

	t.Run("iterate", func(t *testing.T) {
		mock, dt := createTailer(t, 2, 0, 0)

		p1 := mock.ExpectConsumePartition("topic", 0, sarama.OffsetNewest)
		p2 := mock.ExpectConsumePartition("topic", 1, sarama.OffsetNewest)

		p1.YieldMessage(msg("key-a", "10", 1))
		p1.YieldMessage(msg("key-b", "11", 10))
		p2.YieldMessage(msg("key-c", "12", 2))
		p2.YieldMessage(msg("key-c", "13", 3))
		p2.YieldMessage(msg("key-d", "14", 4))

		ctx, cancel := context.WithCancel(context.Background())

		var (
			done   = make(chan struct{})
			runErr error
		)
		go func() {
			defer close(done)
			runErr = dt.Run(ctx)
		}()

		waitDelay(t, func() bool {
			offsets, err := dt.getOffsets([]int32{0, 1})
			require.NoError(t, err)
			return offsets[0] == 0 && offsets[1] == 1
		})

		var (
			m      sync.Mutex
			keys   []string
			values []string
		)
		handler := func(item *Item[string]) HandleResult {
			m.Lock()
			defer m.Unlock()
			keys = append(keys, item.Key)
			val, _ := item.Value()
			values = append(values, val)
			return Continue
		}

		t.Run("iterate with duplicates", func(t *testing.T) {
			// reset iteration-result
			keys = nil
			values = nil

			// iterate with duplicates
			stats := IterStats{}
			err := dt.Iterate(ctx, false, &stats, handler)
			require.NoError(t, err)
			require.EqualValues(t, 5, stats.ItemsIterated())
			require.EqualValues(t, now.Add(1*time.Second), stats.OldestItem())
			require.EqualValues(t, 0, stats.UniqueKeys()) // 0 as we did not deduplicate
			require.ElementsMatch(t, []string{"key-b", "key-d", "key-c", "key-c", "key-a"}, keys)
			require.ElementsMatch(t, []string{"11", "14", "13", "12", "10"}, values)
		})
		t.Run("iterate without duplicates", func(t *testing.T) {
			// reset iteration-result
			keys = nil
			values = nil

			// iterate with duplicates
			stats := IterStats{}
			err := dt.Iterate(ctx, true, &stats, handler)
			require.NoError(t, err)
			require.EqualValues(t, 5, stats.ItemsIterated())
			require.EqualValues(t, now.Add(1*time.Second), stats.OldestItem())
			require.EqualValues(t, 4, stats.UniqueKeys()) // 0 as we did not deduplicate
			require.ElementsMatch(t, []string{"key-b", "key-d", "key-c", "key-a"}, keys)
			require.ElementsMatch(t, []string{"11", "14", "13", "10"}, values)
		})

		cancel()
		<-done
		require.NoError(t, runErr)
	})

	t.Run("load-offsets", func(t *testing.T) {
		mock, dt := createTailer(t, 1, 0, 0)

		ctx, cancel := context.WithCancel(context.Background())

		var (
			done   = make(chan struct{})
			runErr error
		)

		// offset 3 is stored, so we'll expect offset 4 to be consumed
		require.NoError(t, dt.storeOffset(0, 3))
		p1 := mock.ExpectConsumePartition("topic", 0, 4)

		// emit some more messages
		p1.YieldMessage(msg("e", "f", 15)) // 4
		p1.YieldMessage(msg("e", "f", 15)) // 5
		p1.YieldMessage(msg("e", "f", 15)) // 6

		go func() {
			defer close(done)
			runErr = dt.Run(ctx)
		}()

		waitDelay(t, func() bool {
			offsets, err := dt.getOffsets([]int32{0})
			require.NoError(t, err)

			return offsets[0] == 6
		})

		cancel()
		<-done
		require.NoError(t, runErr)
	})

	t.Run("clean", func(t *testing.T) {

		mock, dt := createTailer(t, 1, time.Second, 2*time.Second)

		dt.config.Clk.(*clock.Mock).Set(time.Now())

		p1 := mock.ExpectConsumePartition("topic", 0, sarama.OffsetNewest)

		p1.YieldMessage(msg("key-a", "10", 0))
		p1.YieldMessage(msg("key-a", "10", 0))
		p1.YieldMessage(msg("key-a", "10", 4))

		ctx, cancel := context.WithCancel(context.Background())

		var (
			done   = make(chan struct{})
			runErr error
		)
		go func() {
			defer close(done)
			runErr = dt.Run(ctx)
		}()

		waitDelay(t, func() bool {
			offsets, err := dt.getOffsets([]int32{0})
			require.NoError(t, err)
			return offsets[0] == 1
		})

		var (
			iterated atomic.Int64
		)
		handler := func(item *Item[string]) HandleResult {
			iterated.Add(1)
			return Continue
		}

		t.Run("iterate with duplicates", func(t *testing.T) {

			// let the items flow in
			time.Sleep(100 * time.Millisecond)

			stats := IterStats{}
			iterated.Store(0)
			err := dt.Iterate(ctx, false, &stats, handler)
			require.NoError(t, err)
			require.EqualValues(t, 3, stats.ItemsIterated())
			require.EqualValues(t, 3, iterated.Load())

			// move forward 1.5 seconds, the cleaner should have been run once, but not removed anything
			dt.config.Clk.(*clock.Mock).Add(1500 * time.Millisecond)
			stats = IterStats{}
			iterated.Store(0)
			err = dt.Iterate(ctx, false, &stats, handler)
			require.NoError(t, err)
			require.EqualValues(t, 3, stats.ItemsIterated())
			require.EqualValues(t, 3, iterated.Load())

			// move forward 1.5 seconds, cleaner should have cleaned the last two values
			dt.config.Clk.(*clock.Mock).Add(1500 * time.Millisecond)
			stats = IterStats{}
			iterated.Store(0)
			err = dt.Iterate(ctx, false, &stats, handler)
			require.NoError(t, err)
			require.EqualValues(t, 1, stats.ItemsIterated())
			require.EqualValues(t, 1, iterated.Load())

			// move forward 1.5 seconds, cleaner should have cleaned the last two values
			dt.config.Clk.(*clock.Mock).Add(5 * time.Second)
			stats = IterStats{}
			iterated.Store(0)
			err = dt.Iterate(ctx, false, &stats, handler)
			require.NoError(t, err)
			require.EqualValues(t, 0, stats.ItemsIterated())
			require.EqualValues(t, 0, iterated.Load())
		})

		cancel()
		<-done
		require.NoError(t, runErr)
	})

}

func waitDelay(t *testing.T, waitFor func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if waitFor() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("operation timed out")
}

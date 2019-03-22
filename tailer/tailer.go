package tailer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
)

type TailMessageHook func(message *sarama.ConsumerMessage)

// Tailer retrieves the last n messages from a given topic
type Tailer struct {
	size          int64
	topic         string
	client        sarama.Client
	consumer      sarama.Consumer
	partConsumers []sarama.PartitionConsumer

	tailHook TailMessageHook

	m        sync.RWMutex
	numItems int64
	items    []*sarama.ConsumerMessage
	codec    goka.Codec
}

func defaultTailHook(*sarama.ConsumerMessage) {}

// Stop the tailer
func (t *Tailer) Stop() {
	defer t.client.Close()
	defer t.consumer.Close()
	for _, cons := range t.partConsumers {
		defer cons.Close()
	}
}

// NewTailer creates a new tailer for passed topic, size and codec
func NewTailer(brokers []string, topic string, size int, codec goka.Codec) (*Tailer, error) {
	if size <= 0 {
		return nil, errors.New("Tailer size must be greater than zero")
	}
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Error creating sarama client: %v", err)
	}

	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("Error creating sarama consumer: %v", err)
	}

	return &Tailer{
		topic:    topic,
		size:     int64(size),
		client:   client,
		codec:    codec,
		tailHook: defaultTailHook,
		consumer: c,
	}, nil
}

// RegisterConsumeHook registers a TailMessageHook hook
func (t *Tailer) RegisterConsumeHook(tailHook TailMessageHook) {
	t.tailHook = tailHook
}

// Start starts the tailer
func (t *Tailer) Start() error {
	partitions, err := t.consumer.Partitions(t.topic)
	if err != nil {
		return fmt.Errorf("Error determining the number of partitions for topic %s: %v", t.topic, err)
	}

	if len(partitions) == 0 {
		return fmt.Errorf("Topic %s seems to have no partitions", t.topic)
	}
	offsetsPerPartition := t.size / int64(len(partitions))

	for _, partition := range partitions {
		oldestOffset, err1 := t.client.GetOffset(t.topic, partition, sarama.OffsetOldest)
		newestOffset, err2 := t.client.GetOffset(t.topic, partition, sarama.OffsetNewest)
		if err1 != nil || err2 != nil {
			return fmt.Errorf("Error determining offsets for %s/%d: (%v .. %v)", t.topic, partition, err1, err2)
		}

		// try to get the offset we're supposed to start reading from
		offset := newestOffset - offsetsPerPartition
		// limit to zero
		if offset < 0 {
			offset = 0
		}
		// if its earlier than oldest, use that instead
		if offset < oldestOffset {
			offset = oldestOffset
		}

		// start consuming from it
		partConsumer, err := t.consumer.ConsumePartition(t.topic, partition, offset)
		if err != nil {
			return fmt.Errorf("error consuming %s/%d from offset %d: %v", t.topic, partition, offset, err)
		}
		t.partConsumers = append(t.partConsumers, partConsumer)
		go t.tail(partConsumer)
	}
	return nil
}

func (t *Tailer) tail(partConsumer sarama.PartitionConsumer) {
	for msg := range partConsumer.Messages() {
		t.addMessage(msg)
	}
}

func (t *Tailer) addMessage(msg *sarama.ConsumerMessage) {
	t.m.Lock()
	defer t.m.Unlock()

	t.tailHook(msg)

	if t.numItems < int64(t.size) {
		t.items = append(t.items, msg)
	} else {
		t.items[t.numItems%int64(t.size)] = msg
	}
	t.numItems++
}

// EndOffset specifies the largest offset. It is used to tell IterateReverse to
// return all items independent of their offset
const (
	EndOffset int64 = -1
)

var (
	StopErr = errors.New("Iteration stopped.")
)

// IterateReverse iterates over all items ignoring items having bigger offset than maxOffset
// (or all, if EndOffset is given)
func (t *Tailer) IterateReverse(maxOffset int64, visit func(item interface{}, kafkaOffset int64) error) error {
	t.m.RLock()
	defer t.m.RUnlock()

	if t.numItems == 0 {
		return nil
	}

	numIterations := t.numItems
	if numIterations > t.size {
		numIterations = t.size
	}
	// save the last index (without modulo) for later so we don't have to
	// calculate it in every round
	numItemsIdx := t.numItems + t.size - 1

	for i := int64(0); i < numIterations; i++ {
		idx := (numItemsIdx - i) % t.size
		item := t.items[idx]
		if maxOffset > EndOffset && item.Offset > maxOffset {
			continue
		}
		decoded, err := t.codec.Decode(item.Value)
		if err != nil {
			return fmt.Errorf("Error decoding message %+v with codec %v: %v", item, t.codec, err)
		}
		if err := visit(decoded, item.Offset); err != nil {
			if err == StopErr {
				return nil
			}
			return err
		}
	}
	return nil
}

func (t *Tailer) Read(num int64, offset int64) ([]interface{}, error) {
	t.m.RLock()
	defer t.m.RUnlock()
	if offset < 0 {
		return nil, errors.New("Offset must be >=0")
	}

	var (
		numItems   int64 // actual number of items we have in the list
		firstIndex int64 // first index to return results from [exclusive]
		lastIndex  int64 // last index to return results from [inclusive]
		idxShift   int64 // how to shift the index to get around the ring
	)

	// if the ring is full
	if t.numItems > t.size {
		// --> numItems is its size
		numItems = t.size
		// also we'll shift according to the numItems' mod-value
		idxShift = t.numItems % t.size
	} else {
		// the ring is not full, no shifting occurs
		numItems = t.numItems
	}

	// first define the last index
	lastIndex = numItems - offset - 1
	// we do not have enough items for the offset. Return empty
	if lastIndex < 0 {
		return nil, nil
	}

	// num is negative -> we should return all items
	if num < 0 {
		firstIndex = -1
	} else { // otherwise we'll return how much is requested
		firstIndex = lastIndex - num
	}

	// if there are not enough elements to return, just return
	// everything there's left.
	if firstIndex < -1 {
		firstIndex = -1
	}

	// prepare the list with appropriate capacity
	items := make([]interface{}, 0, lastIndex-firstIndex)

	// iterate from lastIndex to firstindex
	for idx := lastIndex; idx > firstIndex; idx-- {
		// get the real index by shifting + modulo it's size.
		// that way we iterate backwards through the ring
		ringIdx := (idx + idxShift) % t.size

		// decode the element and add it
		decoded, err := t.codec.Decode(t.items[ringIdx].Value)
		if err != nil {
			return nil, fmt.Errorf("Error decoding item while reading from the Tailer: %v", err)
		}
		items = append(items, decoded)
	}

	return items, nil
}

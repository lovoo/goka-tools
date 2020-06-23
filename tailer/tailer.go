package tailer

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"

	"github.com/lovoo/goka"
)

// TailMessageHook is called on every message being added to the tailer
type TailMessageHook func(item *TailerItem)

// TailerVisiter is called on reverseiterate for every message
type TailerVisiter func(item *TailerItem) error

// TailerItem represents a decoded messagei n the tailer's ring buffer
type TailerItem struct {
	// Key is the key of the original message
	Key string
	// Value is the decoded value. This will not be nil, as the tailer ignores nils
	Value interface{}
	// Offset is the message's offset
	Offset int64
}

// Tailer updates messages from a topic and keeps them in a ring buffer for reverse iteration
type Tailer struct {
	size          int64
	topic         string
	client        sarama.Client
	consumer      sarama.Consumer
	partConsumers []sarama.PartitionConsumer

	tailHook TailMessageHook

	m        sync.RWMutex
	numItems int64
	items    []*TailerItem
	codec    goka.Codec
}

func defaultTailHook(item *TailerItem) {}

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

// RegisterConsumeHook sets the callback that will be called on every message being added to the tailer
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
		err := t.addMessage(msg)
		if err != nil {
			log.Printf("Error decoding message: %v", err)
		}
	}
}

func (t *Tailer) addMessage(msg *sarama.ConsumerMessage) error {
	t.m.Lock()
	defer t.m.Unlock()

	if msg.Value == nil {
		return nil
	}

	decoded, err := t.codec.Decode(msg.Value)
	if err != nil {
		return fmt.Errorf("Error decoding message %+v with codec %v: %v", msg, t.codec, err)
	}

	item := &TailerItem{
		Key:    string(msg.Key),
		Offset: msg.Offset,
		Value:  decoded,
	}

	t.tailHook(item)

	if t.numItems < int64(t.size) {
		t.items = append(t.items, item)
	} else {
		t.items[t.numItems%int64(t.size)] = item
	}
	t.numItems++
	return nil
}

// EndOffset specifies the largest offset. It is used to tell IterateReverse to
// return all items independent of their offset
const (
	EndOffset int64 = -1
)

var (
	// ErrStop indicates the iteration should be stopped. This can be returned from the iterate-Visitor
	ErrStop = errors.New("iteration stopped")
)

// IterateReverse iterates over all items ignoring items having bigger offset than maxOffset
// (or all, if EndOffset is given)
func (t *Tailer) IterateReverse(maxOffset int64, visit TailerVisiter) error {
	t.m.RLock()
	defer t.m.RUnlock()

	if t.numItems == 0 {
		return fmt.Errorf("kafka topic does not have msg")
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
		if err := visit(item); err != nil {
			if err == ErrStop {
				return nil
			} else {
				return err
			}
		}
	}
	return nil
}

// AllItems indicates all items from the tailer should be read
const AllItems = -1

func (t *Tailer) Read(num int64, offset int64) ([]*TailerItem, error) {
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
	items := make([]*TailerItem, 0, lastIndex-firstIndex)

	//fmt.Printf("num=%d, offset=%d, numItems=%d, first=%d, last=%d, items-len=%d, items-cap=%d\n", num, offset, numItems, firstIndex, lastIndex, len(items), cap(items))

	// iterate from lastIndex to firstindex
	for idx := lastIndex; idx > firstIndex; idx-- {
		// get the real index by shifting + modulo it's size.
		// that way we iterate backwards through the ring
		ringIdx := (idx + idxShift) % t.size
		items = append(items, t.items[ringIdx])
	}

	return items, nil
}

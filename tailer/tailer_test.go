package tailer

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
)

const allItems = -1

func TestTailer_addMessage(t *testing.T) {
	tailer := &Tailer{
		tailHook: defaultTailHook,
		size:     3,
	}

	// add items until we're full
	ensure.True(t, tailer.numItems == 0)
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 1})
	ensure.True(t, tailer.numItems == 1)
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 2})
	ensure.True(t, tailer.numItems == 2)
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 3})
	ensure.True(t, tailer.numItems == 3)
	// check correct order
	ensure.True(t, tailer.items[0].Offset == 1)
	ensure.True(t, tailer.items[1].Offset == 2)
	ensure.True(t, tailer.items[2].Offset == 3)

	// add one more
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 4})
	ensure.True(t, tailer.numItems == 4)
	ensure.True(t, tailer.items[0].Offset == 4)
	ensure.True(t, tailer.items[1].Offset == 2)
	ensure.True(t, tailer.items[2].Offset == 3)

}

func TestTailer_Read(t *testing.T) {
	// create a test tailer using goka's string codec
	tailer := &Tailer{
		size:     3,
		codec:    new(codec.String),
		tailHook: defaultTailHook,
	}

	// helper function testing to read from the tailer and verifying the results
	testRead := func(count int64, offset int64, expectedResults []string, caseNum string) {
		items, err := tailer.Read(count, offset)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, len(items), len(expectedResults), "case", caseNum)

		for idx, expected := range expectedResults {
			ensure.DeepEqual(t, items[idx].(string), expected, "case", caseNum)
		}
	}

	// read on empty tailer
	testRead(0, 0, []string{}, "1")
	testRead(1, 0, []string{}, "2")
	testRead(1, 1, []string{}, "3")
	testRead(allItems, 0, []string{}, "4")

	// add one message, and test again
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 1, Value: []byte("1")})
	testRead(0, 0, []string{}, "5")
	testRead(1, 0, []string{"1"}, "6")
	testRead(10000, 0, []string{"1"}, "6")
	testRead(1, 1, []string{}, "7")
	testRead(2, 0, []string{"1"}, "8")
	testRead(1, 1, []string{}, "9")
	testRead(allItems, 0, []string{"1"}, "10")

	// add more than the tailer can save and test again
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 2, Value: []byte("2")})
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 3, Value: []byte("3")})
	tailer.addMessage(&sarama.ConsumerMessage{Offset: 4, Value: []byte("4")})
	testRead(0, 0, []string{}, "11")
	testRead(1, 0, []string{"4"}, "12")
	testRead(1, 1, []string{"3"}, "13")
	testRead(2, 0, []string{"4", "3"}, "14")
	testRead(1, 3, []string{}, "15")
	testRead(allItems, 0, []string{"4", "3", "2"}, "16")
	testRead(allItems, 1, []string{"3", "2"}, "17")
	testRead(allItems, 2, []string{"2"}, "18")
}

func TestTailer_IterateReverse(t *testing.T) {
	tailer := &Tailer{
		size:     3,
		codec:    new(codec.String),
		tailHook: defaultTailHook,
	}

	iterateWithOffset := func(offset int) []string {
		var read []string
		tailer.IterateReverse(int64(offset), func(item interface{}, kafkaOffset int64) error {
			read = append(read, item.(string))
			return nil
		})
		return read
	}

	iterate := func() []string {
		return iterateWithOffset(allItems)
	}

	var offset int64
	nextOffset := func() int64 {
		offset++
		return offset
	}

	ensure.DeepEqual(t, len(iterate()), 0)

	tailer.addMessage(&sarama.ConsumerMessage{Offset: nextOffset(), Value: []byte("a")})
	ensure.DeepEqual(t, iterate(), []string{"a"})
	ensure.DeepEqual(t, iterateWithOffset(1), []string{"a"})
	tailer.addMessage(&sarama.ConsumerMessage{Offset: nextOffset(), Value: []byte("b")})
	ensure.DeepEqual(t, iterate(), []string{"b", "a"})
	ensure.DeepEqual(t, iterateWithOffset(1), []string{"a"})
	ensure.DeepEqual(t, iterateWithOffset(2), []string{"b", "a"})
	tailer.addMessage(&sarama.ConsumerMessage{Offset: nextOffset(), Value: []byte("c")})
	ensure.DeepEqual(t, iterate(), []string{"c", "b", "a"})
	tailer.addMessage(&sarama.ConsumerMessage{Offset: nextOffset(), Value: []byte("d")})
	ensure.DeepEqual(t, iterate(), []string{"d", "c", "b"})

	tailer.addMessage(&sarama.ConsumerMessage{Offset: nextOffset(), Value: []byte("e")})
	ensure.DeepEqual(t, iterate(), []string{"e", "d", "c"})
	ensure.DeepEqual(t, iterateWithOffset(6), []string{"e", "d", "c"})
	ensure.DeepEqual(t, iterateWithOffset(5), []string{"e", "d", "c"})
	ensure.DeepEqual(t, iterateWithOffset(4), []string{"d", "c"})
	ensure.DeepEqual(t, iterateWithOffset(3), []string{"c"})
	ensure.DeepEqual(t, len(iterateWithOffset(2)), 0)
}

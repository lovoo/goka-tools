package telly

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
)

func (t *Telly) offsetForPartition(offsets *offsetDoc, partition int32, tmgr goka.TopicManager) (int64, error) {
	if storedOffset := offsets.Offsets[partition]; storedOffset != 0 {

		oldest, err := tmgr.GetOffset(t.opts.topic, partition, sarama.OffsetOldest)
		if err != nil {
			return 0, fmt.Errorf("error getting oldest offset for %s/%d: %v", t.opts.topic, partition, err)
		}
		if storedOffset < oldest {
			return oldest, nil
		}

		newest, err := tmgr.GetOffset(t.opts.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return 0, fmt.Errorf("error getting newest offset for %s/%d: %v", t.opts.topic, partition, err)
		}
		if storedOffset > newest {
			return newest, nil
		}

		return storedOffset, nil
	}

	if t.opts.loadFromPast < 0 {
		return sarama.OffsetOldest, nil
	}

	if t.opts.loadFromPast == 0 {
		return sarama.OffsetNewest, nil
	}

	startTime := time.Now().Add(-t.opts.loadFromPast).Unix() * 1000

	offset, err := tmgr.GetOffset(t.opts.topic, partition, startTime)
	if err != nil {
		return 0, fmt.Errorf("Error finding offset for %s/%d at time %d: %v", t.opts.topic, partition, startTime, err)
	}

	return offset, nil
}

func (t *Telly) readPartition(ctx context.Context, part int32, cons sarama.Consumer, offset int64, inputMsgChan chan<- *sarama.ConsumerMessage) error {

	partCons, err := cons.ConsumePartition(t.opts.topic, part, offset)
	if err != nil {
		return fmt.Errorf("error creating partition consumer %s/%d: %v", t.opts.topic, part, err)
	}
	defer func() {
		log.Printf("reading partition done")
		if err := partCons.Close(); err != nil {
			log.Printf("error closing partition consumer %s/%d: %v", t.opts.topic, part, err)
		}
	}()

	errs := partCons.Errors()
	msgs := partCons.Messages()

	for {
		select {
		case err, ok := <-errs:
			if !ok {
				return nil
			}
			if err != nil {
				log.Printf("error consuming %s/%d: %v", t.opts.topic, part, err)
			}
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			// Special case for the goka.tester.Tester, that sends a nil-value for every message to
			// flush the channels.
			if msg == nil {
				continue
			}

			// handle message
			select {
			case inputMsgChan <- msg:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

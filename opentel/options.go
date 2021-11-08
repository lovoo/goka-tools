package opentel

import (
	"context"
	"hash"

	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"github.com/lovoo/goka"
)

type OtelBuilder struct {
	cfg             *sarama.Config
	producerBuilder goka.ProducerBuilder
	consumerBuilder goka.SaramaConsumerBuilder
	groupBuilder    goka.ConsumerGroupBuilder
}

func NewBuilder() *OtelBuilder {
	return &OtelBuilder{
		cfg:             goka.DefaultConfig(),
		producerBuilder: goka.DefaultProducerBuilder,
		consumerBuilder: goka.DefaultSaramaConsumerBuilder,
		groupBuilder:    goka.DefaultConsumerGroupBuilder,
	}
}

// wrapper that wraps the consumer group, that wraps the consumer-group-handler with otel support
type wrappingConsumerGroup struct {
	sarama.ConsumerGroup
}

func (wcg *wrappingConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	wrappedHandler := otelsarama.WrapConsumerGroupHandler(handler)
	return wcg.ConsumerGroup.Consume(ctx, topics, wrappedHandler)
}

func (ob *OtelBuilder) WithConfig(cfg *sarama.Config) *OtelBuilder {
	ob.cfg = cfg
	ob.consumerBuilder = goka.SaramaConsumerBuilderWithConfig(cfg)
	ob.producerBuilder = goka.ProducerBuilderWithConfig(cfg)
	ob.groupBuilder = goka.ConsumerGroupBuilderWithConfig(cfg)
	return ob
}

func (ob *OtelBuilder) WithConsumerGroupBuilder(groupBuilder goka.ConsumerGroupBuilder) *OtelBuilder {
	ob.groupBuilder = groupBuilder
	return ob
}

func (ob *OtelBuilder) WithConsumerBuilder(consBuilder goka.SaramaConsumerBuilder) *OtelBuilder {
	ob.consumerBuilder = consBuilder
	return ob
}

func (ob *OtelBuilder) BuildProcOptions() []goka.ProcessorOption {
	return []goka.ProcessorOption{
		goka.WithConsumerGroupBuilder(func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
			consGroup, err := ob.groupBuilder(brokers, group, clientID)
			return &wrappingConsumerGroup{
				ConsumerGroup: consGroup,
			}, err
		}),
		goka.WithProducerBuilder(func(brokers []string, clientID string, hasher func() hash.Hash32) (goka.Producer, error) {
			prod, err := ob.producerBuilder(brokers, clientID, hasher)
			return prod, err
			// fix producer
			// return otelsarama.WrapSyncProducer(ob.cfg, prod), err
		}),
		goka.WithConsumerSaramaBuilder(func(brokers []string, clientID string) (sarama.Consumer, error) {
			cons, err := ob.consumerBuilder(brokers, clientID)
			return otelsarama.WrapConsumer(cons), err
		}),
	}
}

func (ob *OtelBuilder) BuildViewOptions() []goka.ViewOption {
	return []goka.ViewOption{
		goka.WithViewConsumerSaramaBuilder(func(brokers []string, clientID string) (sarama.Consumer, error) {
			cons, err := ob.consumerBuilder(brokers, clientID)
			return otelsarama.WrapConsumer(cons), err
		}),
	}
}

func (ob *OtelBuilder) BuildEmitterOptions() []goka.EmitterOption {
	return []goka.EmitterOption{
		goka.WithEmitterProducerBuilder(func(brokers []string, clientID string, hasher func() hash.Hash32) (goka.Producer, error) {
			prod, err := ob.producerBuilder(brokers, clientID, hasher)
			return prod, err
			// fix producer
			// return otelsarama.WrapAsyncProducer(ob.cfg, prod), err
		}),
	}
}

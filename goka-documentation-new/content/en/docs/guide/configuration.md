---
title: "Configuration"
linkTitle: "Configuration"
type: docs
weight: 20
description: >
  How to configure Goka and Sarama
---

This page gives an overview and some hints how to configure Goka and Sarama - the library used for communicating with Kafka

## Configuration

Goka relies on [Sarama](https://github.com/Shopify/sarama) to perform the actual communication with Kafka, which offers many configuration settings. The config is documented [here](https://godoc.org/github.com/Shopify/sarama#Config).

In most cases, you need to modify the config, e.g. to set the Kafka Version.

```go
cfg := goka.DefaultConfig()
cfg.Version = sarama.V2_4_0_0
goka.ReplaceGlobalConfig(cfg)
```

This makes all goka components use the updated config.

If you do need specific configuration for different components, you need to pass customized builders to the
component's constructor, e.g.

```go
cfg := goka.DefaultConfig()
// modify the config with component-specific settings

// use the config by creating a builder which allows to override global config
goka.NewProcessor(// ...,
  goka.WithConsumerGroupBuilder(
    goka.ConsumerGroupBuilderWithConfig(cfg),
  ),
  // ...
)
```

## Kafka Version

## Hasher

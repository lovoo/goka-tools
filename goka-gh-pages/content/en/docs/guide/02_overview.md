---
title: "Overview"
linkTitle: "Overview"
type: docs
weight: 2
description: >
  Write your first Emitter, Processor and View
---

Goka encourages the developer to decompose the application into microservices using three different components: emitters, processors, and views.
The figure below depicts the abstract application again, but now showing the use of these three components together with Kafka and the external API.

![simple application](/goka/goka-arch-simple.png)

**Emitters.** Part of the API offers operations that can modify the state.
Calls to these operations are transformed into streams of messages with the help of an emitter, i.e., the state modification is persisted before performing the actual action as in the [event sourcing pattern](https://martinfowler.com/eaaDev/EventSourcing.html).
An *emitter* emits an event as a key-value message to Kafka.
In Kafka's parlance, emitters are called producers and messages are called records.
We employ the modified terminology to focus this discussion to the scope of Goka only.
Messages are grouped in topics, e.g., a topic could be a type of click event in the interface of the application. In Kafka, topics are partitioned and the message's key is used to calculate the partition into which the message is emitted.

**Processors.** A *processor* is a set of callback functions that modify the content of a key-value table upon the arrival of messages. A processor consumes from a set of input topics (i.e., input streams).
Whenever a message *m* arrives from one of the input topics, the appropriate callback is invoked.
The callback can then modify the table's value associated with *m*'s key.

**Processor groups.** Multiple instances of a processor can partition the work of consuming the input topics and updating the table.
These instances are all part of the same processor group.
A *processor group* is Kafka's consumer group bound to the table it modifies.

**Group table and group topic.** Each processor group is bound to a single table (that represents its state) and has exclusive write-access to it.
We call this table the *group table*.
The *group topic* keeps track of the group table updates, allowing for recovery and rebalance of processor instances as described later. Each processor instance keeps the content of the partitions it is responsible for in its local storage, by default [LevelDB](https://github.com/syndtr/goleveldb).
A local storage in disk allows a small memory footprint and minimizes the recovery time.

**Views.** A *view* is a persistent cache of a group table. A view subscribes to the updates of all partitions of a group topic, keeping a local disk storage in sync with the table. With a view, one can easily serve up-to-date content of the group table via, for example, [gRPC](https://github.com/grpc/grpc-go).

As we present next, the composability, scalability, and fault-tolerance aspects of Goka are strongly related to Kafka. For example, emitters, processors, and views can be deployed in different hosts and scaled in different ways because they communicate exclusively via Kafka. Before discussing these aspects though, we take a look at a simple example.


Package API documentation is available at [GoDoc] and the [Wiki](https://github.com/lovoo/goka/wiki/Tips#configuring-log-compaction-for-table-topics) provides several tips for configuring, extending, and deploying Goka applications.

## Installation

You can install Goka by running the following command:

``$ go get -u github.com/lovoo/goka``

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

## Concepts

Goka relies on Kafka for message passing, fault-tolerant state storage and workload partitioning.

* **Emitters** deliver key-value messages into Kafka. As an example, an emitter could be a database handler emitting the state changes into Kafka for other interested applications to consume.

* **Processor** is a set of callback functions that consume and perform state transformations upon delivery of these emitted messages. *Processor groups* are formed of one or more instances of a processor. Goka distributes the partitions of the input topics across all processor instances in a processor group. This enables effortless scaling and fault-tolerance. If a processor instance fails, its partitions and state are reassigned to the remaining healthy members of the processor group. Processors can also emit further messages into Kafka.

* **Group table** is the state of a processor group. It is a partitioned key-value table stored in Kafka that belongs to a single processor group. If a processor instance fails, the remaining instances will take over the group table partitions of the failed instance recovering them from Kafka.

* **Views** are local caches of a complete group table. Views provide read-only access to the group tables and can be used to provide external services for example through a gRPC interface.

* **Local storage** keeps a local copy of the group table partitions to speedup recovery and reduce memory utilization. By default, the local storage uses [LevelDB](https://github.com/syndtr/goleveldb), but in-memory map and [Redis-based storage](https://github.com/lovoo/goka/tree/master/storage/redis) are also available.

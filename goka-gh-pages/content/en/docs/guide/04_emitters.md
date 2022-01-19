---
title: "Emitters"
linkTitle: "Emitters"
type: docs
weight: 20
description: >
  Emitters deliver key-value messages into a particular topic in Kafka. As an example, an emitter could be a database handler emitting the state changes into Kafka for other interested applications to consume.
---

**TODO: Chart**

Setting up an emitter requires
* a list of brokers
* a target topic
* a target [Codec](https://pkg.go.dev/github.com/lovoo/goka#Codec)


{{< highlight go "linenos=table">}} 
emitter, err := goka.NewEmitter([]string{"localhost:9092"}, "topic-name", new(codecs.Int64))

{{< /highlight >}}




**Emitters.** Part of the API offers operations that can modify the state.
Calls to these operations are transformed into streams of messages with the help of an emitter, i.e., the state modification is persisted before performing the actual action as in the [event sourcing pattern](https://martinfowler.com/eaaDev/EventSourcing.html).
An *emitter* emits an event as a key-value message to Kafka.
In Kafka's parlance, emitters are called producers and messages are called records.
We employ the modified terminology to focus this discussion to the scope of Goka only.
Messages are grouped in topics, e.g., a topic could be a type of click event in the interface of the application. In Kafka, topics are partitioned and the message's key is used to calculate the partition into which the message is emitted.

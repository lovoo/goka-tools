---
title: "Using Processors"
linkTitle: "Using Processors"
weight: 10
description: >
  How processors work and how to set them up
---


## Fundamentals

A processor consumes messages from one or more input topics and produces messages to none or many output topics.

It processor can also have it's own state.

{{< figure src="/goka/processor-plain.png" title="simplified processor" >}}

In Kafka, topics are divided into partitions, each message belonging to a distinct partition. 

{{< figure src="/goka/processor-simple.png" title="single processor" >}}


In a goka processor consuming a topic, we create one *PartitionProcessor* for each partition of the input topic *running in its own goroutine*.

That means, **Goka processes messages in parallel across partitions, but sequentially within one partition**. 

State

If multiple topics are consumed, they 'share' the partition processors of the same partitions
{{< figure src="/goka/processor-group-multiple.png" title="Single processor, multiple groups" >}}




## Lifecycle


* Load everything inside the table-topic --> it won't get modified in the mean time as this instance will be the only one writing to it.
* start consuming which means -> read state from local disk, writechanges to local disk and the kafka topic
* 

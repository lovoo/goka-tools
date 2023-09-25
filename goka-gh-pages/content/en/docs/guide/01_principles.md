---
title: "Principles"
linkTitle: "Principles"
type: docs
weight: 1
description: >
  The concept and motiviation behind Goka
---

This page describes basic components and design principles that help understanding goka and its usage. Goka makes use of the concept [database inside out](https://martin.kleppmann.com/2015/11/05/database-inside-out-at-oredev.html), introduced by Martin Kleppmann in 2015.

--> This page should contain:

- what are the building blocks (--> take from overview)
- what are design decisions (like keystrings?)
- what's the concept of write-by-proc and view-caches?

Then:

- processors-in-depth --> all options, all graph-edges
  --> what's inside the callback?
  
- emitters --> all options
- views --> all options, reconnecting etc.

- show testing shortly --> link to example
- show monitoring --> link to example

## Processors write tables

## Concepts

Goka relies on Kafka for message passing, fault-tolerant state storage and workload partitioning.

- **Emitters** deliver key-value messages into Kafka. As an example, an emitter could be a database handler emitting the state changes into Kafka for other interested applications to consume.

- **Processor** is a set of callback functions that consume and perform state transformations upon delivery of these emitted messages. *Processor groups* are formed of one or more instances of a processor. Goka distributes the partitions of the input topics across all processor instances in a processor group. This enables effortless scaling and fault-tolerance. If a processor instance fails, its partitions and state are reassigned to the remaining healthy members of the processor group. Processors can also emit further messages into Kafka.

- **Group table** is the state of a processor group. It is a partitioned key-value table stored in Kafka that belongs to a single processor group. If a processor instance fails, the remaining instances will take over the group table partitions of the failed instance recovering them from Kafka.

- **Views** are local caches of a complete group table. Views provide read-only access to the group tables and can be used to provide external services for example through a gRPC interface.

- **Local storage** keeps a local copy of the group table partitions to speedup recovery and reduce memory utilization. By default, the local storage uses [LevelDB](https://github.com/syndtr/goleveldb), but in-memory map and [Redis-based storage](https://github.com/lovoo/goka/tree/master/storage/redis) are also available.

## Codecs

### Why are keys always `string`

Kafka uses types `[]byte` internally for both key and value of a message.

Goka uses `string` to identify keys only. This was an early design decision to simplify usage and avoid the need for two codecs.

If the application does require keys to be encoded from objects, one could instantiate a [Codec]({{< ref "/docs/guide/01_principles#Codecs" >}}) in places where the key needs to be read or written.

{{< highlight go "linenos=table" >}}
keyCodec := NewKeyCodec()

proc, err := goka.Processor([]string{"localhost:9092"},
  goka.DefineGroup("input-topic",NewValueCodec(), func(ctx goka.Context, msg interface{}){
    // use codec for decoding the key
    key, err := keyCodec.Decode([]byte(ctx.Key()))
    if err != nil{
      ctx.Fail(err)
    }

    // use keyCodec to encode for writing
    encoded, err := keyCodec.Encode("new-key")
    if err != nil{
      ctx.Fail(err)
    }
    ctx.Emit("output-topic", string(encodedKey), valueToEmit)
  }),
)

{{< /highlight >}}

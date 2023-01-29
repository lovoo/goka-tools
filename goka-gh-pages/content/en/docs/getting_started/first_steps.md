---
title: "First Steps"
linkTitle: "First Steps"
type: docs
description: >
  Write your first Emitter, Processor and View
---


This page shows simple examples of the three building blocks of Goka to get you up and running in no time. 

Check out the [Guide]({{< ref "/docs/guide" >}}) for an in-depth introduction to all components or dive into more complex [examples](https://github.com/lovoo/goka/tree/master/examples) in the repository.

### Adding data to Kafka

Let's start by pushing some data to a Kafka-Topic.

{{< highlight go "linenos=table">}} 
emitter, err := goka.NewEmitter([]string{"localhost:9092"}, "int64-numbers", new(codecs.Int64))

defer emitter.Finish()

for i:=0;;i++{
  prom, err := emitter.Emit(fmt.Sprintf("key-%d", i%2), int64(i))
  if err != nil{
  // handle  synchronous error
  }

  prom.Then(func(err error)){
    if err != nil{
      // handle asynchronous error
    }
  })

  time.Sleep(100*time.Millisecond)
}
{{< /highlight >}}

So what's happening here? The emitter is more or less a wrapper around a `Sarama AsyncProducer`. It needs
* brokers to connect to
* a target topic
* a [Codec]({{< ref "/docs/guide/01_principles#Codecs" >}}), used to convert to the `[]byte` data which kafka uses

Kafka also `[]byte` for keys as well, but for convenience goka assumes all keys are `string`.

Note that the emitter does not create the topic, if you need to do that manually, see  [Creating kafka topics](#creating-kafka-topics)

The topic now ends up being filled with the key-value pairs: `(key-0, 0)`, `(key-1, 1)`, `(key-0, 2)`, `(key-1, 3)`, `(key-0, 4)`, ... and so on

### Processing data

Here is a simple processor, converting `int64` to `string` and accumulating the incoming numbers by key

{{< highlight go "linenos=table">}} 

proc, err := goka.Processor([]string{"localhost:9092"}, 
  goka.DefineGroup("converter",
    goka.Input("int64-numbers", new(codecs.Int64), func(ctx goka.Context, msg interface{}){
      ctx.Output("string-numbers", ctx.Key(), fmt.Sprintf("%d", msg.(int64)))

    
      if val := ctx.Value(); val == nil{
        ctx.SetValue(msg)
      }else{
        ctx.SetValue(val.(int64)+msg.(int64))
      }
    }),
    goka.Output("output-topic", new(codecs.String)),
    goka.Persist(codec.Int64),
  ),
)

if err != nil{
  // handle error
}

if err := proc.Run(context.Background()); err != nil{
  // handle error
}
{{< /highlight >}}

What do you need to set it up?
* list of brokers
* a [group graph]({{< ref "/docs/guide/03_processor.md" >}})

A group graph configures the *group* of the processor, as well as input and output topics. This is only the most basic example though, check out `Creating a Group Graph` for more details. It's called group graph, since we consider a bunch of goka-components as a *topology* or *graph*.

The *group* of the processor is used by Kafka's rebalance protocol. By default, starting multiple instances with the same *group* will split partitions, so provides a way to scale horizontally.

In the example above, we do the following:
* use `goka.Input` to consume `int64-numbers` (created by the [Emitter](#adding-data-to-kafka) above)

  The provided callback is called for every key-value-pair (in parallel - one go-routine for each partition)
  
  The codec takes care of converting each `[]byte` message to `int64`.
* `Sprintf` it into a `string` and emit it using the same key into `string-numbers`. 

  The output is configured using `goka.Output()`, because we need to specify its name and codec, so we don't have to work with `[]byte`s in the callback.
* Accumulating the incoming values under the same key. 

  Every Goka Processor can define one `group-table`, which makes it a stateful processor. A table can only be modified by the processor defining it. Modifications are limited to the key of the currrently processed key. The value for the current *key* is read by  `ctx.Value()` (or nil if not set) and written by `ctx.SetValue()`.

  Each `group-table` is stored locally using `leveldb` and emitted to kafka-topic, named `converter-table` in our example. This table can be used as `Lookup` by other processors or read by clients using a `View` (see next step)

There is much more you can do with Processors, just check out the Guide.


### Reading a table

Ok. Now we have data, we have processed it (very naively, sure), now we want to provide an API for it. So let's do it.


{{< highlight go "linenos=table">}} 

view, err := goka.NewView([]string{"localhost:9092"}, 
                          goka.GroupTable("converter"), 
                          new(codecs.Int64)

if err != nil{
  // handle error
}

// start it
go func(){
  if err := view.Run(context.Background()); err != nil{
    // handle error
  }
}()



// wait for the view to come up
for{
  time.Sleep(1*time.Second)
  if view.Recovered(){
    break
  }
}

// query the view for key-0
val, err := view.Get("key-0")

if err != nil{
  // handle error
}

if val == nil{
  // does not exist
}else{
  sum := val.(int64)
  // serve sum to the client via http, grpc, or needle printer
}

{{< /highlight >}}

Using a `view` is even simpler, because all you need to do is define its table (which is usually the state of an other processor, so we can reuse it's name) and a codec. 

Waiting for `view.Recovered()` is necessary, because it will essentially download the whole kafka-topic on the first run and must not be used earlier, to avoid it serving outdated data.

Key-Value semantics seems raw and too simple, but in the end it's very powerful and there are very rare cases where other semantics are necessary. We do (or intend to) provide examples to use other data structures as well.


### Starting a local cluster

There are many ways to get up a running Kafka, maybe you already have access to some hosted cluster. If you don't, spin up a cluster locally using `docker-compoe`:

```bash
## going to the examples folder of goka
cd goka/examples

## starting a cluster
make start
```

### Creating kafka topics

Goka provides a convenient component to set up topics manually if necessary

{{< highlight go "linenos=table">}} 

tmgr, err := goka.NewTopicManager([]string{"localhost:9092", cfg.DefaultConfig())
if err != nil{
  // handle error
}

// create a new topic
err := tmgr.EnsureStreamExists("example-topic", 20)
if err != nil{
  // handle error
}
{{< /highlight >}}

Setting up topics is required for 

* Emitter topics
* Processor outputs
* Processor inputs

The reason goka processors (and emitters) do not create those topics automatically is to separate the concerns between processing data and maintaining infrastructure. Only exceptions are:

* processor tables, i.e. log-compacted topics that processors use to store their state
* processor loopbacks

The TopicManager can also set up tables (i.e. topics configured for log compaction), which is usually done by the `Processor` automatically. Sometimes it's necessary to be done manually:

* if you start adding a view without a running processor
* if the company policy does not allow creating topics during run time


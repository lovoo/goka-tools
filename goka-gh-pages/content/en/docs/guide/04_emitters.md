---
title: "Emitters"
linkTitle: "Emitters"
type: docs
weight: 20
description: >
  Emitters deliver key-value messages into a particular topic in Kafka. As an example, an emitter could be a database handler emitting the state changes into Kafka for other interested applications to consume.
---


{{< figure src="/goka/emitter.png" title="Emitter" >}}

Setting up an emitter requires
* a list of brokers
* a target topic
* a target {{< links/gokadoc codec "#Codec" >}}

{{< highlight go "linenos=table">}} 
emitter, err := goka.NewEmitter([]string{"localhost:9092"}, "topic-name", new(codecs.Int64))
// handle error

// wait for all outstanding messages to be sent
err := emitter.Finish()
// handle error

{{< /highlight >}}

Emitters write key-value pairs into a specific kafka topic. Encoding the message is done by a {{< links/gokadoc codec "#Codec" >}}.

The emitter determines the partition based on the key using a `hasher` passed to the underlying sarama partitioner {{< links/saramadoc partitioner "#Partitioner" >}}.


Messages can be sent 

* asynchronously (using the underlying producer's batching) using {{< links/gokadoc Emit "#Emitter.Emit" >}} or 

  Asynchronous calls return a {{< links/gokadoc EmitSync "#Promise" >}} that can be used to subscribe on success or failure of the operation.

{{< highlight go "linenos=table">}} 
prom, err := emitter.Emit("key", int64(123))
// handle error

prom.Then(func(err error){
  if err != nil{
    // handle asynchronous error
  }
})
{{< /highlight >}}

* synchronously using {{< links/gokadoc EmitSync "#Emitter.EmitSync" >}}. The call returns when the message was sent or failed.
  
{{< highlight go "linenos=table">}} 
err := emitter.EmitSync("key", int64(124))
// handle error
{{< /highlight >}}



+++
title="why goka"

+++

## Philosophy

Goka supports developers to build scalable, fault-tolerant, data-intensive applications based on [Apache Kafka](https://kafka.apache.org/).

Goka is a Golang twist of the ideas described in ["I heart logs"](http://shop.oreilly.com/product/0636920034339.do) by Jay Kreps and ["Making sense of stream processing"](http://www.oreilly.com/data/free/stream-processing.csp) by Martin Kleppmann.

At the core of any Goka application are one or more key-value tables representing the application state.

Goka provides building blocks to manipulate such tables in a composable, scalable, and fault-tolerant manner.

All state-modifying operations are transformed in event streams, which guarantee key-wise sequential updates.

Read-only operations may directly access the application tables, providing eventually consistent reads.


## Features
  * **Message Input and Output**

    Goka handles all the message input and output for you. You only have to provide one or more callback functions that handle messages from any of the Kafka topics you are interested in. You only ever have to deal with deserialized messages.

  * **Scaling**

    Goka automatically distributes the processing and state across multiple instances of a service. This enables effortless scaling when the load increases.

  * **Fault Tolerance**

    In case of a failure, Goka will redistribute the failed instance's workload and state across the remaining healthy instances. All state is safely stored in Kafka and messages delivered with *at-least-once* semantics.

  * **Built-in Monitoring and Introspection**

    Goka provides a web interface for monitoring performance and querying values in the state.

  * **Modularity**

    Goka fosters a pluggable architecture which enables you to replace for example the storage layer or the Kafka communication layer.
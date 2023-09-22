---
title: "Tips"
linkTitle: "Tips"
type: docs
weight: 1
description: >
  Tips and tricks
---

This is a list of notes on how to configure and use Goka.
You should check these notes before putting Goka into production.

The only configuration that affects correctness is the first one and *must* be applied.

## Configuring log compaction for table topics

By default, topics are not log compacted in Kafka.
Goka tables should be configured with log compaction, otherwise Kafka gradually deletes their content based on the default retention time.

Here are some ways to configure a table topic with log compaction.

### Using TopicManager to create table topics
Goka provides a topic manager to help the creation of tables with log compaction on.
At the time of writting, Sarama does not support the creation of topics via Kafka, so we need to connect to ZooKeeper to create them.

When creating a processor pass a `ZKTopicManagerBuilder` as option
```go
servers := []string{"localhost:2181/chroot"}
p, err := goka.NewProcessor(brokers, graph,
    goka.WithTopicManagerBuilder(
        kafka.ZKTopicManagerBuilder(servers)))
```

The TopicManager creates the table topic if it does not exist.
The number of partition is inferred by the number of partitions of the input streams.

In production, this is our preferred method to create properly configured tables.
We use to disable the default automatic topic creation of Kafka to avoid having to reconfigure topics later on.

Note that only Goka processors create tables, views don't.

## Manually creating a table topic

Instead of letting a TopicManager automatically create tables, one can manually create them (or using scripts).
For a group called `mygroup`, one should create a table topic called `mygroup-table`,
for example:

```sh
./kafka-topics.sh                       \
    --zookeeper localhost:2181      \
    --create                        \
    --replication-factor 2          \
    --partitions 10                 \
    --config cleanup.policy=compact \
    --topic mygroup-table
```

Be sure to create the table with the same number of partitions as the input streams of the processor group.

## Configuring an existing table topic

If the table topic already exists in Kafka, one may configure it for log compaction as follows.

```sh
./kafka-topics.sh                       \
    --zookeeper localhost:2181      \
    --alter                         \
    --config cleanup.policy=compact \
    --topic mygroup-table
```

We typically use Docker to execute Kafka and ZooKeeper commands.
To execute them from a Docker container, one has to call verbose commands starting with `docker run --rm -it ...`.
In our examples directory, we have a [script](https://github.com/lovoo/goka/tree/master/examples/create-kafka-commands.sh)
that make transparent the use of Docker:
For Kafka command, it creates one local script that internally calls docker and runs the
command with the same name.

## Configuring the Kafka client library

Goka abstracts the Kafka client library and allows one to plug different implementations via options to processors, consumers, and emitters.
The builders in the subpackage `kafka` allow one also to configure the default client library ([sarama](https://github.com/Shopify/sarama)).

## Passing options to Sarama

An often changed configuration is the Kafka version in the Sarama library.
One can change the version as well as any other configuration in Sarama as follows.

```go
config := kafka.NewConfig()
config.Version = sarama.V1_0_0
emitter, _ := goka.NewEmitter(brokers, stream, codec,
    goka.WithEmitterProducerBuilder(
        kafka.ProducerBuilderWithConfig(config)))
```

### Using Confluent Kafka client

One may also use [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) with Goka.
For details check the [github.com/lovoo/goka/kafka/confluent](https://github.com/lovoo/goka/tree/master/kafka/confluent) subpackage.

## Configuring the local storage

As the Kafka client, the local storage can also be replaced, configured, or extended via options.
Examples of extensions are:

- adding metrics, for example, as a Prometheus collector
- adding an LRU layer to speedup read-most workloads in Views
- making a single database for all partitions instead of one per partition
- replacing the local storage with a remote storage, for example, Redis

### Configuring LevelDB
Simply create an option from the LevelDB package `opt` and pass that to `storage.BuilderWithOptions`.

```go
opts := &opt.Options{
    BlockCacheCapacity:     opt.MiB * 30,
    WriteBuffer:            opt.MiB * 10,
}
path := "/tmp/goka"
builder := storage.BuilderWithOptions(path, opts)

p, _ := goka.NewView(brokers, table, codec,
    goka.WithViewStorageBuilder(builder))
```

Note that if you run multiple instances of the same processor group in the same machine, you have to configure different storage path for them.

### Using in-memory local storage

Goka provides a in-memory local storage, which is basically a map.

```go
builder := storage.MemoryBuilder()
p, _ := goka.NewView(brokers, table, codec,
    goka.WithViewStorageBuilder(builder))
```

### Adding an LRU cache around the local storage

In some situations, if the number of reads is higher than the number of writes, just increasing the `BlockCacheCapacity` does not provide a reasonable performance.
One example are views that are often accessed to respond to RPC requests.

Here is a simplified implementation of an LRU cache wrapper around Goka's local storage.

```go
package storagecache

import (
    "fmt"

    lru "github.com/hashicorp/golang-lru"
    "github.com/lovoo/goka"
    "github.com/lovoo/goka/storage"
    "github.com/syndtr/goleveldb/leveldb/opt"
)

// CachedStorage wraps a Goka Storage with an LRU cache.
type CachedStorage struct {
    storage.Storage
    cache *lru.Cache
}

func NewCachedStorage(size int, st storage.Storage) (storage.Storage, error) {
    c, err := lru.New(size)
    if err != nil {
        return nil, fmt.Errorf("error creating cache: %v", err)
    }
    return &CachedStorage{st, c}, nil
}

func (cs *CachedStorage) Has(key string) (bool, error) {
    if cs.cache.Contains(key) {
        return true, nil
    }
    return cs.Storage.Has(key)
}

func (cs *CachedStorage) Get(key string) ([]byte, error) {
    if val, has := cs.cache.Get(key); has {
        return val.([]byte), nil
    }
    val, err := cs.Storage.Get(key)
    cs.cache.Add(key, val)
    return val, err
}

func (cs *CachedStorage) Set(key string, value []byte) error {
    defer cs.cache.Remove(key)
    return cs.Storage.Set(key, value)
}

func (cs *CachedStorage) Delete(key string) error {
    defer cs.cache.Remove(key)
    return cs.Storage.Delete(key)
}

func CachedStorageBuilder(size int, path string, opts *opt.Options) goka.StorageBuilder {
    builder := storage.BuilderWithOptions(path, opts)
    return func(topic string, partition int32) (storage.Storage, error) {
        st, err := builder(topic, partition)
        if err != nil {
            return err
        }
        return NewCachedStorage(size, st)
    }
}
```

To create the view with the cache layer, one should pass the builder returned by `storagecache.CachedStorageBuilder` to the `goka.WithViewStorageBuilder` option.
With such wrappers one can introduce instrumentation as well as other features to the local storage.

Note that if you are using iterators you should also handle them to have a consistent snapshot of the table.

## Further tips

### Add monitoring and statistics

This [example](https://github.com/lovoo/goka/tree/master/examples/8-monitoring) shows how to add a web monitoring interface as well as a query interface to processors and views.

Per-partition statistics can be gathered from processors and views with the `Stats()` method.
Low-level statistics from Sarama can be gathered by accessing the go-metrics registry passed to Sarama in the configuration object.
Low-level statistics from the local storage can be gathered by wrapping the storage as shown above and passing the respective storage builder.

### Set the hasher

If you are running Goka alongside other Kafka consumers and producers, for example, Kafka Streams, you should set the hasher to be murmur2 otherwise keys won't be sent to the expected partitions (Sarama uses another the fnv hash function by default).

### Terminate processors gracefully

Killing a processor may cause a long time to rebalance since this has to be detected and propagated within Kafka.
We prefer catching all signals and stopping processors as well as views.

### Make your usage patterns and extensions a library

We use to wrap all our extensions and usage patterns in an internal library.
The library helps processors and services to have a consistent structure.
Among other things, our internal library

- controls the life cycle of processors, views and emitters
- adds Goka's monitoring and query interfaces to all components
- adds pprof endpoints when appropriate
- sets the hasher when appropriate
- wraps the local storage with an LRU cache when desired
- provides Prometheus collectors for local storage, Goka, and Sarama statistics
- configures LevelDB options
- emits per component heartbeats into a special Kafka topic for monitoring and global graph overview
- defines default command line flags such as brokers, zookeepers, storage path, etc.

All these items are very specific to our needs and follow our internal standards.
We recommend creating such a library if you plan to develop large Goka-based applications.

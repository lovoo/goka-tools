# BBQ

BBQ is a library that writes messages from a Kafka topic directly into BigQuery.
To use it, we need to define a list of `TableOptions` which specifies the input topic we
wish to write, the topic's object and its codec, and the expiration time of the table in which
the message will be written.

The table will be automatically created, and its name will be equal to the topic's name. If the structure of the
object being written changes, the table's schema will be automatically updated. However, and this is a BigQuery limitation, if a field of the object is removed, the schema of the table won't be updated, and it will produce an error.

## Example

***
The following code is a working example of BBQ using [Goka](https://github.com/lovoo/goka) as our Kafka processing library.

```go
package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/lovoo/bbq"
	"github.com/lovoo/goka"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"time"
)

// tableOptions retypes the bbq.TableOptions-Type to allow extracting
// a list of goka-edges to initialize the processor.
type tableOptions []*bbq.TableOptions

func (tt tableOptions) edges(consumer goka.ProcessCallback) []goka.Edge {
	var edges []goka.Edge
	for _, option := range tt {
		edges = append(edges, goka.Input(option.Input, option.Codec, consumer))
	}
	return edges
}

func main() {
	tables := []*bbq.TableOptions{
		&bbq.TableOptions{
			Input:            "topic_name",
			Obj:              []byte{},
			TimePartitioning: &bigquery.TimePartitioning{Expiration: 14 * 24 * time.Hour},
			Codec:            new(bbq.BytesCodec),
		},
	}

	bbq, _ := bbq.NewBbq("gcp-project", tables, "metrics-namespace")

	prometheus.DefaultRegisterer.MustRegister(bbq)
	proc, err := goka.NewProcessor([]string{"kafka", "brokers"}, goka.DefineGroup(
		"user-group",
		tableOptions(tables).edges(bbq.Consume)...,
	), goka.WithClientID("antispam-bbq"))

	if err != nil {
		panic(err)
	}

	done := make(chan bool)
	go func() {
		defer close(done)
		if err = proc.Run(context.Background()); err != nil {
			log.Fatalf("error running processor: %v", err)
		}
	}()

	bbq.Stop(10 * time.Second)
}

```
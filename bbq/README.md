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
	"context"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka-tools/bbq"
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

	bbq, err := bbq.NewBbq("gcp-project", tables)
	if err != nil {
		log.Fatalf("Unable to create new BBQ: %v", err)
	}

	proc, err := goka.NewProcessor([]string{"kafka", "brokers"}, goka.DefineGroup(
		"bbq-group",
		tableOptions(tables).edges(bbq.Consume)...,
	), goka.WithClientID("antispam-bbq"))

	if err != nil {
		log.Fatalf("Unable to create Goka processor: %v", err)
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
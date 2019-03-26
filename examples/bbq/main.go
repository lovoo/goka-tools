package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka-tools/bbq"
	"github.com/lovoo/goka/codec"
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
			Codec:            new(codec.Bytes),
		},
	}

	bbq, err := bbq.NewBbq("gcp-project", "target-dataset", tables)
	if err != nil {
		log.Fatalf("Unable to create new BBQ: %v", err)
	}

	proc, err := goka.NewProcessor([]string{"kafka", "brokers"}, goka.DefineGroup(
		"bbq-group",
		tableOptions(tables).edges(bbq.Consume)...,
	), goka.WithClientID("bbq"))

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

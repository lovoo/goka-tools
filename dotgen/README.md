# DotGen

DotGen is a package for generating DOT files that describe a Goka GroupGraph.

## Example

***
The following code is a working example on how to generate a DOT using DotGen.

```go
package main

import (
	"flag"
	"log"
	"os"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka-tools/dotgen"
	"github.com/lovoo/goka/codec"
)

var (
	brokers = []string{"localhost:9092"}
	output  = flag.String("output", "stdout", "file to output dot to. stdout if not set")
)

func main() {
	flag.Parse()

	out, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Error opening output file")
	}
	defer out.Close()
	tree := dotgen.NewTree(out)

	// create a sample processor
	_, err = goka.NewProcessor(brokers, goka.DefineGroup(
		goka.Group("sample-processor"),
		goka.Input("sample-input-topic", new(codec.String), func(ctx goka.Context, msg interface{}) {}),
		goka.Output("sample-output-topic", new(codec.String)),
		goka.Persist(new(codec.String)),
	),
		// the processor option WithGroupGraphHook obtain the group graph once the processor is started
		goka.WithGroupGraphHook(tree.TrackGroupGraph), goka.WithClientID("sample-processor"),
	)

	if err != nil {
		log.Fatalf("Error creating processor from goka: %v", err)
	}

	tree.Render()
}
```

The resulting DOT of this processor, after being converted to an image (see the example sections), looks like this:
<img src="dotgen/graph.png"  alt="graph"/>
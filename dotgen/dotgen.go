package dotgen

import (
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"

	"github.com/lovoo/goka"
)

// Topic represents a kafka topic (stream or table)
type Topic struct {
	ID      string
	Name    string
	Codec   string
	IsTable bool
}

func (t *Topic) renderNode() string {
	color := "lavender"
	if t.IsTable {
		color = "pink"
	}
	return fmt.Sprintf(`"%s" [shape="Mrecord" label="%s" style="filled" fillcolor="%s"];`, t.ID, t.Name, color)
}

// Processor represents a goka processor
type Processor struct {
	ID        string
	Name      string
	Output    []*Topic
	Input     []*Topic
	LoopCodec string
	Join      []*Topic
	Lookup    []*Topic
	Persist   *Topic
}

func (p *Processor) renderNode() string {
	return fmt.Sprintf(`"%s" [shape="box" label="%s" style="filled" fillcolor="lightcyan"];`, p.ID, p.Name)
}
func (p *Processor) renderEdges() []string {
	var edges []string

	for _, output := range p.Output {
		edges = append(edges, fmt.Sprintf(`"%s" -> "%s" [fontsize=10];`, p.ID, output.ID))
	}
	for _, input := range p.Input {
		edges = append(edges, fmt.Sprintf(`"%s" -> "%s" [fontsize=10];`, input.ID, p.ID))
	}
	for _, join := range p.Join {
		edges = append(edges, fmt.Sprintf(`"%s" -> "%s" [label="join" fontsize=10];`, join.ID, p.ID))
	}
	for _, lookup := range p.Lookup {
		edges = append(edges, fmt.Sprintf(`"%s" -> "%s" [label="lookup" fontsize=10];`, lookup.ID, p.ID))
	}

	if p.LoopCodec != "" {
		edges = append(edges, fmt.Sprintf(`"%s" -> "%s" [label="loop" fontsize=10 constraint=false];`, p.ID, p.ID))
	}

	if p.Persist != nil {
		edges = append(edges, fmt.Sprintf(`"%s" -> "%s" [label="persist" fontsize=10 constraint=false];`, p.ID, p.Persist.ID))
	}

	return edges
}

// Tree stores all processors and topics to be rendered together in one graph
type Tree struct {
	Processors map[string]*Processor
	Topics     map[string]*Topic
	out        io.Writer
}

// NewTree creates a new tree for an output writer
func NewTree(out io.Writer) *Tree {
	return &Tree{
		out:        out,
		Processors: make(map[string]*Processor),
		Topics:     make(map[string]*Topic),
	}
}

func (t *Tree) renderLine(line string) {
	t.out.Write([]byte(line))
	t.out.Write([]byte("\n"))
}

func (t *Tree) renderLines(lines []string) {
	for _, line := range lines {
		t.renderLine(line)
	}
}

// Render renders the different parts of the dot graph
func (t *Tree) Render() {
	t.renderLine("digraph main{")
	t.renderLine("\tedge[arrowhead=vee]")
	t.renderLine("\tgraph [rankdir=LR,compound=true,ranksep=1.0];")

	var procIds []string
	for id := range t.Processors {
		procIds = append(procIds, id)
	}
	t.renderLine(fmt.Sprintf(`{rank=same; "%s";}`, strings.Join(procIds, `" "`)))

	var (
		streamTopics []string
		tableTopics  []string
	)
	for _, topic := range t.Topics {
		if topic.IsTable {
			tableTopics = append(tableTopics, topic.ID)
		} else {
			streamTopics = append(streamTopics, topic.ID)
		}
	}
	if len(streamTopics) > 0 {
		t.renderLine(fmt.Sprintf(`{rank=source; "%s";}`, strings.Join(streamTopics, `" "`)))
	}
	if len(tableTopics) > 0 {
		t.renderLine(fmt.Sprintf(`{rank=sink; "%s";}`, strings.Join(tableTopics, `" "`)))
	}

	for _, proc := range t.Processors {
		t.renderLine(proc.renderNode())
	}
	for _, topic := range t.Topics {
		t.renderLine(topic.renderNode())
	}
	for _, proc := range t.Processors {
		t.renderLines(proc.renderEdges())
	}
	t.renderLine("}")
}

func (t *Tree) TrackGroupGraph(gg *goka.GroupGraph) {
	name := string(gg.Group())
	id := makeID(name)
	if _, exists := t.Processors[id]; exists {
		log.Fatalf(`duplicate processor "%s"`, gg.Group())
	}

	proc := &Processor{
		ID:     id,
		Name:   makeName(name),
		Output: t.getOrCreateTopicsFromEdges(gg.OutputStreams()),
		Input:  t.getOrCreateTopicsFromEdges(gg.InputStreams()),
		Join:   t.getOrCreateTopicsFromEdges(gg.JointTables()),
		Lookup: t.getOrCreateTopicsFromEdges(gg.LookupTables()),
	}
	if gg.LoopStream() != nil {
		proc.LoopCodec = codecName(gg.LoopStream().Codec())
	}
	if gg.GroupTable() != nil {
		proc.Persist = t.getOrCreateTopic(gg.GroupTable().Topic(), codecName(gg.GroupTable().Codec()))
	}
	t.Processors[id] = proc
}

func (t *Tree) getOrCreateTopicsFromEdges(edges goka.Edges) []*Topic {
	var topics []*Topic
	for _, edge := range edges {
		topics = append(topics, t.getOrCreateTopic(edge.Topic(), codecName(edge.Codec())))
	}

	return topics
}

func (t *Tree) getOrCreateTopic(name string, codec string) *Topic {
	id := makeID(name)
	if topic, exists := t.Topics[id]; exists {
		if topic.Name != name || topic.Codec != codec {
			log.Printf("Warning: name/codec conflict for topic %s (codec=%s/%s, name=%s/%s)", name, topic.Codec, codec, topic.Name, name)
		}
		return topic
	}
	topic := &Topic{
		ID:      id,
		Name:    makeName(name),
		Codec:   codec,
		IsTable: strings.HasSuffix(name, "-table"),
	}
	t.Topics[id] = topic

	return topic

}

func makeID(str string) string {
	return strings.Replace(str, "-", "_", -1)
}

func makeName(str string) string {
	return str
}

func codecName(codec goka.Codec) string {
	return reflect.TypeOf(codec).Elem().Name()
}

package mock

import (
	context "context"
	"testing"
	time "time"

	"github.com/lovoo/goka"
)

type KeyedMessage struct {
	Key   string
	Value interface{}
}

type GokaMockContext struct {
	t *testing.T

	key       string
	value     interface{}
	headers   map[string][]byte
	offset    int64
	partition int32
	topic     goka.Stream
	timestamp time.Time

	ctx context.Context

	loopbacks []KeyedMessage

	joins   map[goka.Table]interface{}
	lookups map[goka.Table]map[string]interface{}

	emits map[goka.Stream][]KeyedMessage
}

func NewGokaMockContext(t *testing.T) *GokaMockContext {
	return &GokaMockContext{
		t:       t,
		ctx:     context.Background(),
		headers: make(map[string][]byte),
		emits:   make(map[goka.Stream][]KeyedMessage),
		joins:   make(map[goka.Table]interface{}),
		lookups: make(map[goka.Table]map[string]interface{}),
	}
}

func (g *GokaMockContext) Group() goka.Group {
	return goka.Group("mock-group")
}

func (g *GokaMockContext) Reset() {
	g.emits = make(map[goka.Stream][]KeyedMessage)
}

func (g *GokaMockContext) Topic() goka.Stream {
	return g.topic
}
func (g *GokaMockContext) Key() string {
	return g.key
}
func (g *GokaMockContext) Partition() int32 {
	return g.partition
}
func (g *GokaMockContext) Offset() int64 {
	return g.offset
}
func (g *GokaMockContext) Value() interface{} {
	return g.value
}
func (g *GokaMockContext) Headers() goka.Headers {
	return g.headers
}
func (g *GokaMockContext) SetValue(value interface{}, options ...goka.ContextOption) {
	g.value = value
}
func (g *GokaMockContext) Delete(options ...goka.ContextOption) {
	g.value = nil
}
func (g *GokaMockContext) Timestamp() time.Time {
	return g.timestamp
}
func (g *GokaMockContext) Join(topic goka.Table) interface{} {
	return g.joins[topic]
}
func (g *GokaMockContext) Lookup(topic goka.Table, key string) interface{} {
	return g.lookups[topic][key]
}
func (g *GokaMockContext) Emit(topic goka.Stream, key string, value interface{}, options ...goka.ContextOption) {
	g.emits[topic] = append(g.emits[topic], KeyedMessage{
		Key:   key,
		Value: value,
	})
}
func (g *GokaMockContext) Loopback(key string, value interface{}, options ...goka.ContextOption) {
	g.loopbacks = append(g.loopbacks, KeyedMessage{
		Key:   key,
		Value: value,
	})
}
func (g *GokaMockContext) Fail(err error) {
	g.t.Fatalf("%v", err)
}
func (g *GokaMockContext) Context() context.Context {
	return g.ctx
}
func (g *GokaMockContext) DeferCommit() func(error) {
	return func(error) {
	}
}

func (g *GokaMockContext) WithKeyValue(key string, value interface{}) *GokaMockContext {
	g.key = key
	g.value = value
	return g
}

func (g *GokaMockContext) WithHeaders(headers map[string][]byte) *GokaMockContext {
	g.headers = headers
	return g
}
func (g *GokaMockContext) WithOffset(offset int64) *GokaMockContext {
	g.offset = offset
	return g
}
func (g *GokaMockContext) WithTopic(topic goka.Stream) *GokaMockContext {
	g.topic = topic
	return g
}
func (g *GokaMockContext) WithTimestamp(timestamp time.Time) *GokaMockContext {
	g.timestamp = timestamp
	return g
}
func (g *GokaMockContext) WithPartition(partition int32) *GokaMockContext {
	g.partition = partition
	return g
}

func (g *GokaMockContext) WithJoinValue(table goka.Table, value interface{}) *GokaMockContext {
	g.joins[table] = value
	return g
}
func (g *GokaMockContext) WithContext(ctx context.Context) *GokaMockContext {
	g.ctx = ctx
	return g
}
func (g *GokaMockContext) WithLookupValue(table goka.Table, key string, value interface{}) *GokaMockContext {
	tab := g.lookups[table]
	if tab == nil {
		tab = make(map[string]interface{})
		g.lookups[table] = tab
	}
	tab[key] = value
	return g
}

func (g *GokaMockContext) GetEmitForTopic(stream goka.Stream) []KeyedMessage {
	return g.emits[stream]
}

func (g *GokaMockContext) GetAllEmits() map[goka.Stream][]KeyedMessage {
	return g.emits
}

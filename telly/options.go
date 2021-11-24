package telly

import (
	"fmt"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/tester"
)

type options struct {
	dbName string
	table  string
	topic  string
	codec  goka.Codec

	consBuilder  goka.SaramaConsumerBuilder
	tmgrBuilder  goka.TopicManagerBuilder
	dbConnector  func(address string)
	loadFromPast time.Duration
	createDb     bool
	clientID     string
	insertHook   InsertHook

	primaryKeyName string

	// retention cleaner configuration
	retention       time.Duration
	updateFieldName string

	inserterBatchSize int
	inserterChanSize  int
	cleanInterval     time.Duration
}

type InsertHook func(key []byte, value interface{}) interface{}

func identityInsertHook(key []byte, value interface{}) interface{} {
	return value
}

// Option defines an option to modify the behavior of telly
type Option func(*options)

// WithConsumerSaramaBuilder replaces the default consumer group builder
func WithConsumerSaramaBuilder(cgb goka.SaramaConsumerBuilder) Option {
	return func(o *options) {
		o.consBuilder = cgb
	}
}

// WithTopicManagerBuilder replaces the default topic manager.
func WithTopicManagerBuilder(tmb goka.TopicManagerBuilder) Option {
	return func(o *options) {
		o.tmgrBuilder = tmb
	}
}

// WithInsertHook adds a hook that gets called on every new message added to the database
func WithInsertHook(hook InsertHook) Option {
	return func(o *options) {
		o.insertHook = hook
	}
}

// WithTester modifies the
func WithTester(tt *tester.Tester) Option {
	return func(o *options) {
		o.clientID = tt.RegisterView(goka.Table(string(o.topic)), o.codec)
		o.tmgrBuilder = tt.TopicManagerBuilder()
		o.consBuilder = tt.ConsumerBuilder()
		o.inserterBatchSize = 0
		o.inserterChanSize = 0
	}
}

// WithInitialLoad adds loading old data from the topic
// initially into rethinkdb.
// if loadFromPast == -1, it will load from the beginning.
// if loadFromPast == 0, loading will start from the end of the topic.
func WithInitialLoad(loadFromPast time.Duration) Option {
	return func(o *options) {
		o.loadFromPast = loadFromPast
	}
}

// WithRetention runs a cleaner go-routine that cleans entries, oder than passed retention
// updatedFieldName specifies the rethinkdb-field (for nesting, do 'nested.field.timestamp')
// Note: telly does not check, if this column exists or if it actually contains a valid timestamp.
// It blindly deletes every row which has a value "older" than the one provided
func WithRetention(retention time.Duration, updatedFieldName string) Option {
	return func(o *options) {
		o.updateFieldName = updatedFieldName
		o.retention = retention
	}
}

// WithPrimaryKey specifies the field-name that is being used as primary key when creating the table.
// If the table exists and the key is different, Telly will return an error, so changing the key is
// impossible without deleting the table first.
// By default, field name 'id' is used by rethinkdb. If your data type does not provide this field and
// the primary key is not set, rethinkdb will create a new id, thus making it impossible to overwrite entries.
// Note that we cannot use the message-key provided by Kafka. If you need to use the key, add an InsertHook which returns
// a new data structure containing the key as needed.
func WithPrimaryKey(fieldName string) Option {
	return func(o *options) {
		o.primaryKeyName = fieldName
	}
}

// WithClientID overwrites the default client ID to create consumers for sarama
func WithClientID(clientID string) Option {
	return func(o *options) {
		o.clientID = clientID
	}
}

func createOptions(dbName string, table string, topic string, codec goka.Codec, opts ...Option) (*options, error) {
	o := &options{
		dbName: dbName,
		table:  table,
		topic:  topic,
		codec:  codec,

		consBuilder: goka.DefaultSaramaConsumerBuilder,
		tmgrBuilder: goka.DefaultTopicManagerBuilder,

		loadFromPast: 30 * 24 * time.Hour,
		insertHook:   identityInsertHook,
		clientID:     "telly",

		inserterBatchSize: 500,
		inserterChanSize:  1000,
		cleanInterval:     60 * time.Minute,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o, o.validate()
}

func (o *options) validate() error {
	if o.retention < 0 {
		return fmt.Errorf("retention must be >= 0")
	}
	return nil
}

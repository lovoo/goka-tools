package bbq

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/lovoo/goka"
	"golang.org/x/net/context"
)

const (
	uploaderBatchsize     = 1000
	uploaderTimeout       = 1000 * time.Millisecond
	bigqueryUploadTimeout = 10 * time.Second
)

// Bbq writes the contents of kafka topics to bigquery
type Bbq struct {
	*metrics
	uploaders     map[string]*batchedUploader
	stopUploaders chan bool
	uploadersWg   *sync.WaitGroup
}

type saver struct {
	msgToSave interface{}
}

// TableOptions represents one kafka topic and the connected codec
type TableOptions struct {
	Obj              interface{}
	TimePartitioning *bigquery.TimePartitioning
	Input            goka.Stream
	Codec            goka.Codec
	// CustomSchema allows us to specify the table's schema.
	CustomSchema func() (bigquery.Schema, error)
	// CustomObject allows us to modify the input value into another one.
	CustomObject func(interface{}) interface{}
}

// Name returns the name of the topic
func (to *TableOptions) Name() string {
	return string(to.Input)
}

// Save implements the ValueSaver interface used by BQ
func (s *saver) Save() (map[string]bigquery.Value, string, error) {
	values := convertToMap(s.msgToSave)
	return values, "", nil
}
func convertToMap(item interface{}) map[string]bigquery.Value {
	return convertToMapReflect(reflect.ValueOf(item).Elem())
}
func convertToMapReflect(value reflect.Value) map[string]bigquery.Value {
	values := make(map[string]bigquery.Value, value.NumField())

	for i := 0; i < value.NumField(); i++ {

		if !useFieldForExport(value.Type().Field(i)) {
			continue
		}

		switch value.Field(i).Kind() {
		case reflect.Ptr:
			if !value.Field(i).IsNil() && value.Field(i).Elem().Kind() == reflect.Struct {
				values[value.Type().Field(i).Name] = convertToMapReflect(value.Field(i).Elem())
			}
		case reflect.Slice, reflect.Array, reflect.Map:
			var bqValue bigquery.Value

			if value.Type().Field(i).Type.Elem() == reflect.TypeOf(uint8(0)) {
				bqValue = "<binary data>"
			} else {
				jsonString, err := json.Marshal(value.Field(i).Interface())
				if err != nil {
					log.Printf("Error marshaling map (%#v) to json: %v", value, err)
					break
				}
				bqValue = string(jsonString)
			}

			values[value.Type().Field(i).Name] = bqValue
			// If value is an Enum save to bigquery exports string value
			// that is why we unpack integer constans to plain integers
		case reflect.Int, reflect.Int32, reflect.Int64:
			values[value.Type().Field(i).Name] = value.Field(i).Int()

		case reflect.Float64, reflect.Float32:
			floatVal := value.Field(i).Float()
			if math.IsNaN(floatVal) {
				floatVal = 0
			}
			values[value.Type().Field(i).Name] = floatVal
		case reflect.Interface:
			jsonString, err := json.Marshal(value.Field(i).Interface())
			if err != nil {
				log.Printf("Error marshaling interface (%#v) to json: %v", value, err)
				break
			}
			values[value.Type().Field(i).Name] = string(jsonString)
		default:
			values[value.Type().Field(i).Name] = value.Field(i).Interface()

		}
	}

	return values
}

// NewBbq creates a new Bbq struct.
func NewBbq(gcpproject string, datesetName string, tables []*TableOptions, metricsNamespace string) (*Bbq, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, gcpproject)
	if err != nil {
		return nil, fmt.Errorf("error creating BigQuery client: %v", err)
	}

	dataset := client.Dataset(datesetName)
	log.Printf("BigQuery client created successfully. Writing to %s dataset", datesetName)

	m := newMetrics(metricsNamespace)

	uploaders := make(map[string]*batchedUploader)
	stop := make(chan bool, 1)
	var wg sync.WaitGroup

	for _, tableOption := range tables {
		// BigQuery tables names must be alphanumeric or with underscores.
		name := strings.Replace(tableOption.Name(), "-", "_", -1)
		err = createOrUpdateTable(ctx, dataset, name, tableOption)
		if err != nil {
			return nil, fmt.Errorf("error creating table %v:%v", name, err)
		}

		uploaders[name] = newBatchedUploader(stop, &wg, name, dataset.Table(name).Uploader(),
			m.mxTableInserts.WithLabelValues(name), m.mxErrorUpload,
			uploaderBatchsize, uploaderTimeout, tableOption.CustomObject)
	}

	return &Bbq{
		metrics:       m,
		uploaders:     uploaders,
		stopUploaders: stop,
		uploadersWg:   &wg,
	}, nil

}

// Stop drains the batches in the bbq-uploaders and blocks until they're done
func (b *Bbq) Stop(timeout time.Duration) {
	// stop uploaders
	close(b.stopUploaders)

	// wait for them or time out
	done := make(chan struct{})
	go func() {
		b.uploadersWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("BBQ: uploaders fully drained. BBQ is shutting down cleanly")
	case <-time.After(timeout):
		log.Printf("Timeout shutting down bbq, we will lose some data. Sorry.")
	}
}

// Consume consumes the streams
func (b *Bbq) Consume(ctx goka.Context, msg interface{}) {
	b.metrics.mxLagSeconds.Observe(time.Since(ctx.Timestamp()).Seconds())

	table := strings.Replace(string(ctx.Topic()), "-", "_", -1)
	uploader := b.uploaders[table]

	if uploader.customObject != nil {
		msg = uploader.customObject(msg)
	}

	s := &saver{
		msgToSave: msg,
	}
	b.uploaders[table].Upload(s)
}

func setRequiredFalse(schema bigquery.Schema) {
	for _, val := range schema {
		val.Repeated = false
		val.Required = false
		if val.Type == bigquery.RecordFieldType {
			setRequiredFalse(val.Schema)
		}
	}
}

func createOrUpdateTable(ctx context.Context, dataset *bigquery.Dataset, name string, tableOptions *TableOptions) error {
	if name == "" {
		return fmt.Errorf("empty table name")
	}
	// Check if the table exists. If it does not, a new one is created
	table := dataset.Table(name)

	var (
		schema bigquery.Schema
		err    error
	)

	// Do not infer the schema if we give it one
	if tableOptions.CustomSchema != nil {
		schema, err = tableOptions.CustomSchema()
	} else {
		// Infer the schema
		schema, err = inferSchema(tableOptions.Obj)
	}

	//Set all the required fields to false
	setRequiredFalse(schema)
	if err != nil {
		return fmt.Errorf("error infering schema: %v", err)
	}

	metadata, err := table.Metadata(ctx)
	// Error implies that the table does not exist
	if err != nil {
		if metadata == nil {
			metadata = new(bigquery.TableMetadata)
		}

		metadata.Name = name
		metadata.Schema = schema
		metadata.TimePartitioning = tableOptions.TimePartitioning

		err = table.Create(ctx, metadata)
		if err != nil {
			return fmt.Errorf("error creating schema: %v", err)
		}
	} else {
		// If the table exists, the metadata is updated
		if _, err := table.Update(ctx, bigquery.TableMetadataToUpdate{
			Name:   name,
			Schema: schema,
		}, ""); err != nil {
			return fmt.Errorf("error updating table: %v", err)
		}
	}

	return nil
}

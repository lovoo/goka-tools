package bbq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/lovoo/goka-tools/testhelpers"
	"github.com/stretchr/testify/require"
)

type mergeMessage struct {
	A string
	C *messageX
	D *messageA
	E int32
}

type messageA struct {
	A string
	B int32
}

type messageX struct {
	X string
	Y int32
	Z *messageA
}

func TestMergeSchema(t *testing.T) {

	t.Run("merge-schema", func(t *testing.T) {
		schema, err := inferSchema(new(mergeMessage))
		if err != nil {
			t.Fatalf("Error infering schema. %v", err)
		}
		setRequiredFalse(schema)

		metaSchema := bigquery.Schema{
			{Name: "A", Type: "STRING"},
			{Name: "B", Type: "INTEGER"},
			{Name: "C", Type: "RECORD",
				Schema: bigquery.Schema{
					{Name: "Y", Type: "INTEGER"}},
			},
			{Name: "D", Type: "RECORD",
				Schema: bigquery.Schema{
					{Name: "B", Type: "INTEGER"}},
			},
		}

		expectedSchema := bigquery.Schema{
			{Name: "A", Type: "STRING"},
			{Name: "B", Type: "INTEGER"},
			{Name: "C", Type: "RECORD",
				Schema: bigquery.Schema{
					{Name: "Y", Type: "INTEGER"},
					{Name: "X", Type: "STRING"},
					{Name: "Z", Type: "RECORD",
						Schema: bigquery.Schema{
							{Name: "A", Type: "STRING"},
							{Name: "B", Type: "INTEGER"},
						},
					},
				},
			},
			{Name: "D", Type: "RECORD",
				Schema: bigquery.Schema{
					{Name: "B", Type: "INTEGER"},
					{Name: "A", Type: "STRING"},
				},
			},
			{Name: "E", Type: "INTEGER"},
		}

		mergedSchema := mergeFieldsToBqSchema(metaSchema, schema)
		require.Equal(t, 5, len(mergedSchema))
		testhelpers.EnsureEqualStruct(t, mergedSchema, expectedSchema)
	})
}

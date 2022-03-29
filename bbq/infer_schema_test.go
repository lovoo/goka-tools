package bbq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

type sometype struct {
	unexported int
	Exported   int
}

type sometype2 struct {
	Exported2 int
}

func TestInferSchema(t *testing.T) {

	t.Run("ignore-unexported", func(t *testing.T) {
		schema, err := inferSchema(new(sometype))
		if err != nil {
			t.Fatalf("Error infering schema. %v", err)
		}

		if len(schema) != 1 {
			t.Fatalf("schema should contain only the exported field")
		}
		if schema[0].Name != "Exported" {
			t.Fatalf("the wrong field was exported from the struct")
		}
	})

	t.Run("merge-schema", func(t *testing.T) {
		schema, err := inferSchema(new(sometype))
		if err != nil {
			t.Fatalf("Error infering schema. %v", err)
		}

		schema2, err := inferSchema(new(sometype2))
		if err != nil {
			t.Fatalf("Error infering schema. %v", err)
		}

		tests := []struct {
			name           string
			schema         bigquery.Schema
			metaSchema     bigquery.Schema
			expectedSchema bigquery.Schema
		}{
			{
				name:       "add-new",
				schema:     schema,
				metaSchema: bigquery.Schema{},
				expectedSchema: bigquery.Schema{
					{Name: "Exported", Type: "INTEGER"},
				},
			},
			{
				name:   "do-not-add",
				schema: schema2,
				metaSchema: bigquery.Schema{
					{Name: "Exported", Type: "INTEGER"},
					{Name: "Exported2", Type: "STRING"},
				},
				expectedSchema: bigquery.Schema{
					{Name: "Exported", Type: "INTEGER"},
					{Name: "Exported2", Type: "STRING"},
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				require.True(t, matchedFieldName(appendFieldSchema(test.metaSchema, test.schema), test.expectedSchema))
			})
		}
	})
}

func matchedFieldName(actual bigquery.Schema, expected bigquery.Schema) bool {
	for idx, s := range actual {
		if s.Name != expected[idx].Name && string(s.Type) != string(expected[idx].Type) {
			return false
		}
	}
	return true
}

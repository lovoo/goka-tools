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

type original struct {
	A string
	B int32
}

type modifyOriginal struct {
	A string
	C *messageC
	D int32
}

type messageC struct {
	X string
	Y int32
	Z *original
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
		/*
			schema, err := inferSchema(new(original))
			if err != nil {
				t.Fatalf("Error infering schema. %v", err)
			}
		*/

		schema2, err := inferSchema(new(modifyOriginal))
		if err != nil {
			t.Fatalf("Error infering schema. %v", err)
		}

		tests := []struct {
			name           string
			schema         bigquery.Schema
			metaSchema     bigquery.Schema
			expectedLength int
			expectedSchema bigquery.Schema
		}{
			/*
				{
					name:           "add-new",
					schema:         schema,
					metaSchema:     bigquery.Schema{},
					expectedLength: 2,
					expectedSchema: bigquery.Schema{
						{Name: "A", Type: "STRING"},
						{Name: "B", Type: "INTEGER"},
					},
				},
			*/

			{
				name:   "do-not-add",
				schema: schema2,
				metaSchema: bigquery.Schema{
					{Name: "A", Type: "STRING"},
					{Name: "B", Type: "INTEGER"},
					{Name: "C", Type: "RECORD",
						Schema: bigquery.Schema{
							{Name: "Y", Type: "INTEGER"}},
					},
				},
				expectedLength: 4,
				expectedSchema: bigquery.Schema{
					{Name: "A", Type: "STRING"},
					{Name: "B", Type: "INTEGER"},
					{Name: "C", Type: "RECORD",
						Schema: bigquery.Schema{
							{Name: "X", Type: "STRING"},
							{Name: "Y", Type: "INTEGER"},
							{Name: "Z", Type: "RECORD",
								Schema: bigquery.Schema{
									{Name: "A", Type: "STRING"},
									{Name: "B", Type: "INTEGER"},
								},
							},
						},
					},
					{Name: "D", Type: "INTEGER"},
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				require.Equal(t, len(appendFieldSchema(test.metaSchema, test.schema)), test.expectedLength)
				//require.True(t, matchedFieldName(appendFieldSchema(test.metaSchema, test.schema), test.expectedSchema))
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

func TestPopulateSchemaMap(t *testing.T) {
	schema := bigquery.Schema{
		{
			Name: "F1",
			Schema: bigquery.Schema{
				{
					Name: "CF1",
					Schema: bigquery.Schema{
						{
							Name: "SCF1",
						},
					},
				},
				{
					Name: "CF2",
					Schema: bigquery.Schema{
						{
							Name: "SCF1",
							Schema: bigquery.Schema{
								{
									Name: "SSCF1",
								},
							},
						},
					},
				},
			},
		},
		{
			Name: "F2",
			Schema: bigquery.Schema{
				{
					Name: "CF1",
				},
			},
		},
		{
			Name: "F3",
		},
	}
	fieldNames := make(map[string]bool)
	populateFieldNames(fieldNames, nil, schema)
	require.Equal(t, 9, len(fieldNames))
	require.True(t, fieldNames["F1.CF2.SCF1.SSCF1"])
}

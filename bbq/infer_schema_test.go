package bbq

import "testing"

type sometype struct {
	unexported int
	Exported   int
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
}

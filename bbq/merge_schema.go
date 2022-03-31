package bbq

import "cloud.google.com/go/bigquery"

func mergeFieldsToBqSchema(existingSchema, newSchema bigquery.Schema) bigquery.Schema {
	for _, fieldSchema := range newSchema {
		if !isNameExists(existingSchema, fieldSchema.Name) {
			existingSchema = append(existingSchema, fieldSchema)
			continue
		}

		childFieldSchema := getFieldSchemaByName(existingSchema, fieldSchema.Name)
		if childFieldSchema != nil {
			childFieldSchema.Schema = mergeFieldsToBqSchema(childFieldSchema.Schema, fieldSchema.Schema)
		}
	}
	return existingSchema
}

func isNameExists(schema bigquery.Schema, name string) bool {
	for _, fieldSchema := range schema {
		if fieldSchema.Name == name {
			return true
		}
	}
	return false
}

func getFieldSchemaByName(schema bigquery.Schema, name string) *bigquery.FieldSchema {
	for _, fieldSchema := range schema {
		if fieldSchema.Name == name {
			return fieldSchema
		}
	}
	return nil
}

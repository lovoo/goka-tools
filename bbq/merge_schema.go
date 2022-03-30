package bbq

import "cloud.google.com/go/bigquery"

func mergeFieldsToBqSchema(existingSchema, newSchema bigquery.Schema) bigquery.Schema {
	for _, fieldSchema := range newSchema {
		if !isNameExists(existingSchema, fieldSchema.Name) {
			existingSchema = append(existingSchema, fieldSchema)
			continue
		}

		getChildSchema := getSchemaByName(existingSchema, fieldSchema.Name)
		if getChildSchema != nil {
			getChildSchema.Schema = mergeFieldsToBqSchema(getChildSchema.Schema, fieldSchema.Schema)
		}
	}
	return existingSchema
}

func isNameExists(s bigquery.Schema, name string) bool {
	for _, i := range s {
		if i.Name == name {
			return true
		}
	}
	return false
}

func getSchemaByName(schema bigquery.Schema, name string) *bigquery.FieldSchema {
	for _, fieldSchema := range schema {
		if fieldSchema.Name == name {
			return fieldSchema
		}
	}
	return nil
}

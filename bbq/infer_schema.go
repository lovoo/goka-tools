// Taken and altered from https://github.com/googleapis/google-cloud-go
// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bbq

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

var (
	typeOfDate     = reflect.TypeOf(civil.Date{})
	typeOfTime     = reflect.TypeOf(civil.Time{})
	typeOfDateTime = reflect.TypeOf(civil.DateTime{})
	typeOfGoTime   = reflect.TypeOf(time.Time{})
)

var (
	errNoStruct             = errors.New("bigquery: can only infer schema from struct or pointer to struct")
	errUnsupportedFieldType = errors.New("bigquery: unsupported type of field in struct")
	errInvalidFieldName     = errors.New("bigquery: invalid name of field in struct")
)

var typeOfByteSlice = reflect.TypeOf([]byte{})

func inferSchema(st interface{}) (bigquery.Schema, error) {
	rec, err := hasRecursiveType(reflect.TypeOf(st), nil)
	if err != nil {
		return nil, err
	}
	if rec {
		return nil, fmt.Errorf("bigquery: schema inference for recursive type %s", st)
	}
	return inferStruct(reflect.TypeOf(st))
}

func inferStruct(t reflect.Type) (bigquery.Schema, error) {
	switch t.Kind() {
	case reflect.Ptr:
		if t.Elem().Kind() != reflect.Struct {
			return nil, errNoStruct
		}
		t = t.Elem()
		fallthrough

	case reflect.Struct:
		return inferFields(t)
	default:
		return nil, errNoStruct
	}
}

// inferFieldSchema infers the FieldSchema for a Go type
func inferFieldSchema(rt reflect.Type) (*bigquery.FieldSchema, error) {
	switch rt {
	case typeOfByteSlice:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.StringFieldType}, nil
	case typeOfGoTime:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.TimestampFieldType}, nil
	case typeOfDate:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.DateFieldType}, nil
	case typeOfTime:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.TimeFieldType}, nil
	case typeOfDateTime:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.DateTimeFieldType}, nil
	}
	if isSupportedIntType(rt) {
		return &bigquery.FieldSchema{Required: true, Type: bigquery.IntegerFieldType}, nil
	}
	switch rt.Kind() {
	case reflect.Struct, reflect.Ptr:
		nested, err := inferStruct(rt)
		if err != nil {
			return nil, err
		}
		return &bigquery.FieldSchema{Required: true, Type: bigquery.RecordFieldType, Schema: nested}, nil
	case reflect.String:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.StringFieldType}, nil
	case reflect.Bool:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.BooleanFieldType}, nil
	case reflect.Float32, reflect.Float64:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.FloatFieldType}, nil
	case reflect.Slice, reflect.Array, reflect.Map:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.StringFieldType}, nil
	case reflect.Interface:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.StringFieldType}, nil
	default:
		log.Printf("unsupported type for field %s is: %+v", rt.Name(), rt.Kind())
		return nil, errUnsupportedFieldType
	}
}

// inferFields extracts all exported field types from struct type.
func inferFields(rt reflect.Type) (bigquery.Schema, error) {
	var s bigquery.Schema

	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Fields of non-struct type")
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		if !useFieldForExport(field) {
			continue
		}

		inferredSchema, err := inferFieldSchema(field.Type)
		if err != nil {
			return nil, err
		}
		inferredSchema.Name = field.Name
		s = append(s, inferredSchema)
	}
	return s, nil
}

// isSupportedIntType reports whether t can be properly represented by the
// BigQuery INTEGER/INT64 type.
func isSupportedIntType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return true
	default:
		return false
	}
}

// Checks whether a field should be exported. This is both used for the mapping and for
// inferring the schema
func useFieldForExport(field reflect.StructField) bool {
	// field is ignored for marshalling, so let's not export it
	if field.Tag.Get("json") == "-" {
		return false
	}
	if field.Name == "" {
		return false
	}

	// if the field is not exported, drop it
	if strings.ToUpper(field.Name[:1]) != field.Name[:1] {
		return false
	}
	return true
}

// typeList is a linked list of reflect.Types.
type typeList struct {
	t    reflect.Type
	next *typeList
}

func (l *typeList) has(t reflect.Type) bool {
	for l != nil {
		if l.t == t {
			return true
		}
		l = l.next
	}
	return false
}

// hasRecursiveType reports whether t or any type inside t refers to itself, directly or indirectly,
// via exported fields. (Schema inference ignores unexported fields.)
func hasRecursiveType(t reflect.Type, seen *typeList) (bool, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false, nil
	}
	if seen.has(t) {
		return true, nil
	}

	var fields []reflect.StructField

	for i := 0; i < t.NumField(); i++ {
		fields = append(fields, t.Field(i))
	}

	seen = &typeList{t, seen}
	// Because seen is a linked list, additions to it from one field's
	// recursive call will not affect the value for subsequent fields' calls.
	for _, field := range fields {
		ok, err := hasRecursiveType(field.Type, seen)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

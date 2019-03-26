package bbq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/facebookgo/ensure"
)

type messageA struct {
	A string
	B int32
	C *messageB
	D []*messageB
}

type messageB struct {
	A int32
	B map[string]string
}

func TestSave(t *testing.T) {
	a := &messageA{
		A: "message",
	}

	s := &saver{
		msgToSave: a,
	}

	values, _, err := s.Save()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, values["A"], "message")
}

func TestSetRequired(t *testing.T) {
	adviceSchema, err := inferSchema(messageA{})
	ensure.Nil(t, err)

	setRequiredFalse(adviceSchema)
	ensure.False(t, adviceSchema[0].Repeated)
	ensure.False(t, adviceSchema[0].Required)
}

func TestConvertToMap(t *testing.T) {
	msg := &messageA{
		A: "a string",
		B: 4,
		C: &messageB{
			A: 3,
		},
	}

	valuesMap := convertToMap(msg)
	val, ok := valuesMap["C"]
	ensure.True(t, ok)
	msg2, ok := val.(map[string]bigquery.Value)
	ensure.True(t, ok)
	_, ok = msg2["A"]
	ensure.True(t, ok)
}

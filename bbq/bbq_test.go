package bbq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/facebookgo/ensure"
)

func TestSave(t *testing.T) {
	a := &MessageA{
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
	adviceSchema, err := inferSchema(MessageA{})
	ensure.Nil(t, err)

	setRequiredFalse(adviceSchema)
	ensure.False(t, adviceSchema[0].Repeated)
	ensure.False(t, adviceSchema[0].Required)
}

func TestConvertToMap(t *testing.T) {
	msg := &MessageA{
		A: "a string",
		B: 4,
		C: &MessageB{
			A: 3,
		},
		E: MessageC_OTHER,
	}

	valuesMap := convertToMap(msg)
	val, ok := valuesMap["C"]
	ensure.True(t, ok)
	msg2, ok := val.(map[string]bigquery.Value)
	ensure.True(t, ok)
	val, ok = msg2["A"]
	ensure.True(t, ok)
	ensure.DeepEqual(t, int64(3), val)

	val, ok = valuesMap["E"]
	ensure.True(t, ok)
	ensure.DeepEqual(t, int64(MessageC_OTHER), val)

}

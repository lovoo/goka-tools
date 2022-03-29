package bbq

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

func TestSave(t *testing.T) {
	a := &MessageA{
		A: "message",
	}

	s := &saver{
		msgToSave: a,
	}

	values, _, err := s.Save()
	require.Nil(t, err)
	require.Equal(t, values["A"], "message")
}

func TestSetRequired(t *testing.T) {
	adviceSchema, err := inferSchema(MessageA{})
	require.Nil(t, err)

	setRequiredFalse(adviceSchema)
	require.False(t, adviceSchema[0].Repeated)
	require.False(t, adviceSchema[0].Required)
}

func TestConvertToMap(t *testing.T) {
	msg := &MessageA{
		A: "a string",
		B: 4,
		C: &MessageB{
			A: 3,
		},
	}

	valuesMap := convertToMap(msg)
	val, ok := valuesMap["C"]
	require.True(t, ok)
	msg2, ok := val.(map[string]bigquery.Value)
	require.True(t, ok)
	_, ok = msg2["A"]
	require.True(t, ok)
}

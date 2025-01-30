package disktail

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestEndOffsetRange(t *testing.T) {
	key := encodeKey(time.Now(), 0, 100)
	require.EqualValuesf(t, -1, pebble.DefaultComparer.Compare(endOffsetRange, key), "%v, %v, ", endOffsetRange, key)

}
func TestEncodeOffsetKey(t *testing.T) {
	key := encodeOffsetKey(12)

	tt, offset, part := decodeKey(key)

	require.EqualValues(t, 0, tt.UnixMicro())
	require.EqualValues(t, 0, offset)
	require.EqualValues(t, 12, part)
}

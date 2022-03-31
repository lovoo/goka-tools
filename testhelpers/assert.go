package testhelpers

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

// EnsureProtosEqual compares two proto messages using protocmp to get readable comparison results
func EnsureProtosEqual(t *testing.T, a, b interface{}, args ...interface{}) {
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff != "" {
		require.Fail(t, "unexpected difference", append([]interface{}{diff}, args...))
	}
}

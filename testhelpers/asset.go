package testhelpers

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// EnsureEqualStruct compares two messages using protocmp to get readable comparison results
func EnsureEqualStruct(t *testing.T, a, b interface{}, args ...interface{}) {
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff != "" {
		t.Errorf("unexpected difference:\n%v", diff)
		if len(args) != 0 {

			t.Errorf("Arguments:")
			for idx, arg := range args {
				t.Errorf("- %d: %v", idx, arg)
			}
		}
	}
}

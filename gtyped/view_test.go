package gtyped

import (
	"context"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/tester"
	"github.com/stretchr/testify/require"
)

func TestView(t *testing.T) {
	gkt := tester.New(t)

	v, err := NewGView[string, string](nil, "table", new(StringCodec[string]), new(StringCodec[string]), goka.WithViewTester(gkt))
	require.NoError(t, err)
	strv, err := NewGStringView[string](nil, "table2", new(StringCodec[string]), goka.WithViewTester(gkt))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	errg, ctx := multierr.NewErrGroup(ctx)

	errg.Go(func() error {
		return v.Run(ctx)
	})
	errg.Go(func() error {
		return strv.Run(ctx)
	})

	time.Sleep(100 * time.Millisecond)

	t.Run("get-empty", func(t *testing.T) {
		val, err := v.Get("")
		require.NoError(t, err)
		require.EqualValues(t, val, "")
		val, err = strv.Get("")
		require.NoError(t, err)
		require.EqualValues(t, val, "")
	})

	t.Run("values", func(t *testing.T) {
		gkt.Consume("table", "1", "hello")
		gkt.Consume("table2", "1", "world")

		val, err := v.Get("1")
		require.NoError(t, err)
		require.EqualValues(t, val, "hello")
		val, err = strv.Get("1")
		require.NoError(t, err)
		require.EqualValues(t, val, "world")
	})

	cancel()
	require.NoError(t, errg.Wait().ErrorOrNil())
}

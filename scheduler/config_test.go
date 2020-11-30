package scheduler

import "testing"

type testMetric struct {
	added    float64
	observed float64
}

func (tm *testMetric) Add(val float64) {
	tm.added += val
}

func (tm *testMetric) Observe(val float64) {
	tm.observed = val
}

func TestConfig(t *testing.T) {

	t.Run("default", func(t *testing.T) {
		c := NewConfig()
		c.mxExecuteRoundTrips(2.0)
		c.mxZombieEvicted(2.0)
	})

	t.Run("with-values", func(t *testing.T) {
		tm := new(testMetric)

		c := NewConfig().WithMxZombieEvicted(func(val float64) {
			tm.Add(val)
		}).WithMxExecuteRoundTrips(func(val float64) {
			tm.Observe(val)
		}).WithOrderCatchupTimeout(2)
		c.mxExecuteRoundTrips(1)
		c.mxZombieEvicted(2)

		if tm.added != 2.0 {
			t.Errorf("Evict counter in config not assigned")
		}

		if tm.observed != 1.0 {
			t.Errorf("Execute histogram not assigned")
		}

		if c.orderCatchupTimeout != 2 {
			t.Errorf("order catchup timeout not assigned")
		}
	})
}

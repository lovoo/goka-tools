package scheduler

import "time"

// Config configures the running scheduler
type Config struct {
	// ZombieEvicted counts when a zombie order gets evicted
	mxZombieEvicted Count
	// mxThrottleDuplicate is incremented whenever a throttling order ignores a duplicate
	mxThrottleDuplicate Count
	// mxRescheduled is incremented whenever a waiter waits for a message.
	// The type is the number of milliseconds of the waiter's max wait time.
	// So note this is not actually the number of milliseconds the waiter is going to wait, which is probably less.
	mxRescheduled CountForType

	// mxExecuteRoundTrips observes the number of wait-iterations an order has run before being executed.
	mxExecuteRoundTrips Observe

	// mxExecutionTimeDeviation observes the number of seconds that the actual execution time
	// deviates from the planned execution time.
	mxExecutionTimeDeviation Observe

	// mxPlaceOrderLag observes the current lag of processing order placements in seconds
	mxPlaceOrderLag Observe

	// configures the duration after which we'll consider a delay-order being "lost", meaning that because of possible
	// waiter-change we might never get the order to be executed, so we will execute the order right now.
	orderCatchupTimeout time.Duration
}

// Observe metric allows to observe multiple values, e.g. a histogram or a summary
type Observe func(float64)

// Count allows to add values and calculate a rate
type Count func(float64)

// CountForType allows to count events belonging to a specific type
type CountForType func(string, float64)

// NewConfig creates a new config for
func NewConfig() *Config {
	return &Config{
		// init metrics with dummies
		mxZombieEvicted:          func(float64) {},
		mxThrottleDuplicate:      func(float64) {},
		mxRescheduled:            func(string, float64) {},
		mxExecuteRoundTrips:      func(float64) {},
		mxExecutionTimeDeviation: func(float64) {},
		mxPlaceOrderLag:          func(float64) {},

		// after 10 seconds we'll try to do a catchup.
		orderCatchupTimeout: 10 * time.Second,
	}
}

// WithMxZombieEvicted sets a counter for measuring the number of
// zombie orders being evicted.
func (c *Config) WithMxZombieEvicted(cnt Count) *Config {
	c.mxZombieEvicted = cnt
	return c
}

// WithMxExecuteRoundTrips sets an observer for measuring the number of
// round trips an order has done before finally being executed. This will
// usually be a histogram or summary
func (c *Config) WithMxExecuteRoundTrips(h Observe) *Config {
	c.mxExecuteRoundTrips = h
	return c
}

// WithMxExecutionTimeDeviation sets an observer for measuring the seconds of
// deviation between planned execution and actual execution. This will
// usually be a histogram or summary. Times can also be negative in case there
// are no waiters small enough for the last iteration, which means the order will be executed before
// its actual deadline.
func (c *Config) WithMxExecutionTimeDeviation(h Observe) *Config {
	c.mxExecutionTimeDeviation = h
	return c
}

// WithMxThrottleDuplicate sets a counter for measuring the number of
// duplicates/throttles for a throttling order
func (c *Config) WithMxThrottleDuplicate(cnt Count) *Config {
	c.mxThrottleDuplicate = cnt
	return c
}

// WithMxRescheduled sets a counter for measuring the number of
// reschedules in total
func (c *Config) WithMxRescheduled(cnt CountForType) *Config {
	c.mxRescheduled = cnt
	return c
}

// WithOrderCatchupTimeout sets a counter for measuring the number of catchups after
// restarting the scheduler with existing delay-orders
func (c *Config) WithOrderCatchupTimeout(timeout time.Duration) *Config {
	c.orderCatchupTimeout = timeout
	return c
}

// WithPlaceOrderLag sets an observer for measuring the lag in seconds for
// order placement
func (c *Config) WithPlaceOrderLag(o Observe) *Config {
	c.mxPlaceOrderLag = o
	return c
}

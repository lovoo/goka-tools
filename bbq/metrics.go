package bbq

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	subsystem     = "bbq"
	mxErrorUpload = "upload"
)

type metrics struct {
	mxErrorUpload  prometheus.Counter
	mxTableInserts *prometheus.CounterVec
	mxLagSeconds   prometheus.Histogram
}

func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	m.mxErrorUpload.Describe(ch)
	m.mxTableInserts.Describe(ch)
	m.mxLagSeconds.Describe(ch)
}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	m.mxErrorUpload.Collect(ch)
	m.mxTableInserts.Collect(ch)
	m.mxLagSeconds.Collect(ch)
}

func newMetrics(namespace string) *metrics {
	return &metrics{
		mxErrorUpload: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "upload_error",
			Help:      "BBQ upload error counter",
		}),
		mxTableInserts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "table_insert",
			Help:      "BBQ tables insert counter",
		}, []string{"table"}),
		mxLagSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "lag",
			Help:        "Message lag in secondsd",
			Namespace:   namespace,
			Buckets:     []float64{0.0, 0.5, 1.0, 10.0, 60.0, 300.0, time.Hour.Seconds(), (time.Hour * 24).Seconds()},
			ConstLabels: prometheus.Labels{"subsystem": subsystem},
		}),
	}
}

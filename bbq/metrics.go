package bbq

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	subsystem     = "bbq"
	mxErrorUpload = "upload"
)

type metrics struct {
	mxErrorUpload  prometheus.Counter
	mxTableInserts *prometheus.CounterVec
}

func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	m.mxErrorUpload.Describe(ch)
	m.mxTableInserts.Describe(ch)
}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	m.mxErrorUpload.Collect(ch)
	m.mxTableInserts.Collect(ch)
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
	}
}

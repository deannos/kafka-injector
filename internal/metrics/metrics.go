package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	QueueRecords = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "injector_queue_records",
	})

	QueueBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "injector_queue_bytes",
	})

	BatchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "injector_batch_latency_ms",
	})

	KafkaErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "injector_kafka_errors_total",
	})
)

func Register() {
	prometheus.MustRegister(
		QueueRecords,
		QueueBytes,
		BatchLatency,
		KafkaErrors,
	)
}

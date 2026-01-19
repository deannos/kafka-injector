package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	QueueRecords = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "injector",
		Name:      "queue_records",
		Help:      "Number of records currently queued for batching",
	})

	QueueBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "injector",
		Name:      "queue_bytes",
		Help:      "Total bytes currently queued for batching",
	})

	BatchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "injector",
		Name:      "batch_latency_ms",
		Help:      "Kafka batch enqueue latency in milliseconds",
		Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 25, 50},
	})

	KafkaErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "injector",
		Name:      "kafka_errors_total",
		Help:      "Total number of Kafka produce errors",
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

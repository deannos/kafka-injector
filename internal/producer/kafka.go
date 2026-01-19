package producer

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/deannos/kafka-injector/internal/config"
	"github.com/deannos/kafka-injector/internal/metrics"
	"github.com/deannos/kafka-injector/internal/model"
)

type KafkaProducer struct {
	async sarama.AsyncProducer
	topic string
}

/*
Why This Is Mandatory (Important)
Kafka idempotence guarantees:
no duplicates per partition
safe retries
But only if:
1 in-flight request
ordered delivery preserved
Allowing >1 would break ordering → duplicates → corruption.
Sarama package protects you by failing fast.
*/
func NewKafkaProducer(cfg *config.Config, logger *log.Logger) (*KafkaProducer, error) {
	c := sarama.NewConfig()

	// Kafka version (REQUIRED)
	c.Version = sarama.V3_6_0_0

	// Idempotent producer invariants
	c.Producer.Idempotent = true
	c.Net.MaxOpenRequests = 1
	c.Producer.RequiredAcks = sarama.WaitForAll

	// Reliability
	c.Producer.Retry.Max = 10
	c.Producer.Retry.Backoff = 100 * time.Millisecond

	c.Producer.Return.Errors = true
	c.Producer.Return.Successes = false

	//  CRITICAL FIX (correct field for v1.46.x)
	c.Producer.Flush.Messages = 1

	// Optional but recommended
	c.Producer.Compression = sarama.CompressionSnappy

	p, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, c)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range p.Errors() {
			metrics.KafkaErrors.Inc()
			logger.Printf("kafka error: %v", err)
		}
	}()

	return &KafkaProducer{
		async: p,
		topic: cfg.KafkaTopic,
	}, nil
}

func (k *KafkaProducer) SendBatch(records []*model.Record) {
	start := time.Now()
	for _, r := range records {
		k.async.Input() <- &sarama.ProducerMessage{
			Topic: k.topic,
			Key:   sarama.ByteEncoder(r.Key),
			Value: sarama.ByteEncoder(r.Value),
		}
	}
	//  THIS WAS MISSING — record batch enqueue latency
	metrics.BatchLatency.Observe(
		float64(time.Since(start).Milliseconds()) / 1000.0,
	)

}

func (k *KafkaProducer) Close() {
	k.async.Close()
}

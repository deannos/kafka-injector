package producer

import (
	"log"

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

	// REQUIRED for idempotent producer
	c.Producer.Idempotent = true
	c.Net.MaxOpenRequests = 1

	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Retry.Max = int(^uint(0) >> 1) // infinite retries
	c.Producer.Compression = sarama.CompressionSnappy

	c.Producer.Return.Errors = true
	c.Producer.Return.Successes = false

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
	for _, r := range records {
		k.async.Input() <- &sarama.ProducerMessage{
			Topic: k.topic,
			Key:   sarama.ByteEncoder(r.Key),
			Value: sarama.ByteEncoder(r.Value),
		}
	}
}

func (k *KafkaProducer) Close() {
	k.async.Close()
}

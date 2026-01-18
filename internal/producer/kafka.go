package producer

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/deannos/kafka-injector/internal/config"
	"github.com/deannos/kafka-injector/internal/model"
)

type KafkaProducer struct {
	async sarama.AsyncProducer
	topic string
}

func NewKafkaProducer(cfg *config.Config, logger *log.Logger) (*KafkaProducer, error) {
	c := sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Idempotent = true
	c.Producer.Retry.Max = int(^uint(0) >> 1)
	c.Producer.Compression = sarama.CompressionSnappy
	c.Producer.Return.Errors = true

	p, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, c)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range p.Errors() {
			logger.Printf("kafka error: %v", err)
		}
	}()

	return &KafkaProducer{async: p, topic: cfg.KafkaTopic}, nil
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

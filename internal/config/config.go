package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr string

	QueueMaxRecords int
	QueueMaxBytes   int64

	BatchWorkers    int
	BatchMaxRecords int
	BatchMaxBytes   int64
	BatchLinger     time.Duration

	KafkaBrokers []string
	KafkaTopic   string
}

func Load() *Config {
	return &Config{
		HTTPAddr: getEnv("HTTP_ADDR", ":8080"),

		QueueMaxRecords: getEnvInt("QUEUE_MAX_RECORDS", 100_000),
		QueueMaxBytes:   getEnvInt64("QUEUE_MAX_BYTES", 512*1024*1024),

		BatchWorkers:    getEnvInt("BATCH_WORKERS", 8),
		BatchMaxRecords: getEnvInt("BATCH_MAX_RECORDS", 500),
		BatchMaxBytes:   getEnvInt64("BATCH_MAX_BYTES", 1*1024*1024),
		BatchLinger:     time.Millisecond * time.Duration(getEnvInt("BATCH_LINGER_MS", 10)),

		KafkaBrokers: []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		KafkaTopic:   getEnv("KAFKA_TOPIC", "events"),
	}
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func getEnvInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		i, _ := strconv.Atoi(v)
		return i
	}
	return d
}

func getEnvInt64(k string, d int64) int64 {
	if v := os.Getenv(k); v != "" {
		i, _ := strconv.ParseInt(v, 10, 64)
		return i
	}
	return d
}

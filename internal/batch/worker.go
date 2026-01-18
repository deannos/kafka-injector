package batch

import (
	"context"
	"log"
	"time"

	"github.com/deannos/kafka-injector/internal/config"
	"github.com/deannos/kafka-injector/internal/model"
	"github.com/deannos/kafka-injector/internal/queue"
)

type WorkerPool struct {
	cfg      *config.Config
	queue    *queue.BoundedQueue
	producer *producer.KafkaProducer
	logger   *log.Logger
}

func NewWorkerPool(cfg *config.Config, q *queue.BoundedQueue, p *producer.KafkaProducer, l *log.Logger) *WorkerPool {
	return &WorkerPool{cfg: cfg, queue: q, producer: p, logger: l}
}

func (w *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < w.cfg.BatchWorkers; i++ {
		go w.worker(ctx)
	}
}

func (w *WorkerPool) Stop() {}

func (w *WorkerPool) worker(ctx context.Context) {
	var batch []*model.Record
	var bytes int64
	timer := time.NewTimer(w.cfg.BatchLinger)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		w.producer.SendBatch(batch)
		batch = nil
		bytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case r := <-w.queue.Channel():
			batch = append(batch, r)
			bytes += r.Size
			if len(batch) >= w.cfg.BatchMaxRecords || bytes >= w.cfg.BatchMaxBytes {
				flush()
				timer.Reset(w.cfg.BatchLinger)
			}
		case <-timer.C:
			flush()
			timer.Reset(w.cfg.BatchLinger)
		}
	}
}

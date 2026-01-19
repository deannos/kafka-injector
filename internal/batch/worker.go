package batch

import (
	"context"
	"log"
	"time"

	"github.com/deannos/kafka-injector/internal/config"
	"github.com/deannos/kafka-injector/internal/model"
	"github.com/deannos/kafka-injector/internal/producer"
	"github.com/deannos/kafka-injector/internal/queue"
)

type WorkerPool struct {
	cfg      *config.Config
	queue    *queue.BoundedQueue
	producer *producer.KafkaProducer
	logger   *log.Logger
}

func NewWorkerPool(cfg *config.Config, q *queue.BoundedQueue, p *producer.KafkaProducer, l *log.Logger) *WorkerPool {
	return &WorkerPool{
		cfg:      cfg,
		queue:    q,
		producer: p,
		logger:   l,
	}
}

func (w *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < w.cfg.BatchWorkers; i++ {
		id := i
		go w.worker(ctx, id)
	}
}

func (w *WorkerPool) Stop() {
	// Shutdown is driven via context cancellation
}

func (w *WorkerPool) worker(ctx context.Context, id int) {
	w.logger.Printf("BATCH: worker %d started", id)

	var batch []*model.Record
	var bytes int64

	timer := time.NewTimer(w.cfg.BatchLinger)
	defer timer.Stop()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}
		w.logger.Printf(
			"BATCH: worker %d flushing %d records (%d bytes) reason=%s",
			id, len(batch), bytes, reason,
		)

		w.producer.SendBatch(batch)

		batch = nil
		bytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			w.logger.Printf("BATCH: worker %d shutting down", id)
			flush("shutdown")
			return

		case r := <-w.queue.Channel():
			w.logger.Printf("BATCH: worker %d dequeued record size=%d", id, r.Size)

			batch = append(batch, r)
			bytes += r.Size

			if len(batch) >= w.cfg.BatchMaxRecords || bytes >= w.cfg.BatchMaxBytes {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				flush("size")
				timer.Reset(w.cfg.BatchLinger)
			}

		case <-timer.C:
			flush("linger")
			timer.Reset(w.cfg.BatchLinger)
		}
	}
}

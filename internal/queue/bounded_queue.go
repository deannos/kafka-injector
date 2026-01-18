package queue

import (
	"sync"

	"github.com/deannos/kafka-injector/internal/model"
)

type BoundedQueue struct {
	ch chan *model.Record

	maxBytes int64
	curBytes int64
	mu       sync.Mutex
}

func NewBoundedQueue(maxRecords int, maxBytes int64) *BoundedQueue {
	return &BoundedQueue{
		ch:       make(chan *model.Record, maxRecords),
		maxBytes: maxBytes,
	}
}

func (q *BoundedQueue) Enqueue(r *model.Record) {
	for {
		q.mu.Lock()
		if q.curBytes+r.Size <= q.maxBytes {
			q.curBytes += r.Size
			q.mu.Unlock()
			q.ch <- r
			return
		}
		q.mu.Unlock()
	}
}

func (q *BoundedQueue) Dequeue() *model.Record {
	r := <-q.ch
	q.mu.Lock()
	q.curBytes -= r.Size
	q.mu.Unlock()
	return r
}

func (q *BoundedQueue) Channel() <-chan *model.Record {
	return q.ch
}

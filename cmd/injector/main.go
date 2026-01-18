package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/deannos/kafka-injector/internal/batch"
	"github.com/deannos/kafka-injector/internal/config"
	"github.com/deannos/kafka-injector/internal/ingress"
	"github.com/deannos/kafka-injector/internal/producer"
	"github.com/deannos/kafka-injector/internal/queue"
)

func main() {
	cfg := config.Load()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	q := queue.NewBoundedQueue(cfg.QueueMaxRecords, cfg.QueueMaxBytes)
	p, err := producer.NewKafkaProducer(cfg, logger)
	if err != nil {
		logger.Fatal(err)
	}

	wp := batch.NewWorkerPool(cfg, q, p, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go wp.Start(ctx)

	srv := ingress.NewHTTPServer(cfg.HTTPAddr, q, logger)
	go srv.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	cancel()
	wp.Stop()

	ctxShutdown, _ := context.WithTimeout(context.Background(), 10*time.Second)
	srv.Shutdown(ctxShutdown)
	p.Close()
}

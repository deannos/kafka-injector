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
	"github.com/deannos/kafka-injector/internal/metrics"
	"github.com/deannos/kafka-injector/internal/producer"
	"github.com/deannos/kafka-injector/internal/queue"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	logger.Println("BOOT: starting injector")

	cfg := config.Load()
	logger.Printf("BOOT: config loaded: %+v\n", cfg)

	logger.Println("BOOT: registering metrics")
	metrics.Register()

	logger.Println("BOOT: creating bounded queue")
	q := queue.NewBoundedQueue(cfg.QueueMaxRecords, cfg.QueueMaxBytes)

	logger.Println("BOOT: creating kafka producer")
	p, err := producer.NewKafkaProducer(cfg, logger)
	if err != nil {
		logger.Fatalf("BOOT: kafka producer error: %v", err)
	}
	logger.Println("BOOT: kafka producer created")

	logger.Println("BOOT: creating batch worker pool")
	wp := batch.NewWorkerPool(cfg, q, p, logger)

	ctx, cancel := context.WithCancel(context.Background())

	logger.Println("BOOT: starting batch workers")
	go wp.Start(ctx)

	logger.Println("BOOT: starting HTTP server")
	srv := ingress.NewHTTPServer(cfg.HTTPAddr, q, logger)
	go func() {
		if err := srv.Start(); err != nil {
			logger.Printf("HTTP: server exited: %v", err)
		}
	}()

	logger.Printf("BOOT: injector ready, listening on %s\n", cfg.HTTPAddr)

	// ---- SIGNAL HANDLING ----

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	s := <-sig
	logger.Printf("SHUTDOWN: received signal %v\n", s)

	cancel()
	logger.Println("SHUTDOWN: batch workers stopping")
	wp.Stop()

	ctxShutdown, _ := context.WithTimeout(context.Background(), 10*time.Second)
	logger.Println("SHUTDOWN: http server shutting down")
	srv.Shutdown(ctxShutdown)

	logger.Println("SHUTDOWN: kafka producer closing")
	p.Close()

	logger.Println("SHUTDOWN: complete")
}

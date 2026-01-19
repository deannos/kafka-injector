package ingress

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/deannos/kafka-injector/internal/model"
	"github.com/deannos/kafka-injector/internal/queue"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTPServer struct {
	server *http.Server
	queue  *queue.BoundedQueue
	logger *log.Logger
}

func NewHTTPServer(addr string, q *queue.BoundedQueue, logger *log.Logger) *HTTPServer {
	mux := http.NewServeMux()

	// Metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	s := &HTTPServer{
		queue:  q,
		logger: logger,
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	// Ingest endpoint
	mux.HandleFunc("/ingest", s.handleIngest)
	return s
}

func (s *HTTPServer) Start() error {
	s.logger.Printf("HTTP: starting server on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	s.logger.Println("HTTP: shutting down server")
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleIngest(w http.ResponseWriter, r *http.Request) {
	s.logger.Println("HTTP: /ingest handler entered")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	// ---- single-pass decode ----
	var payload interface{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&payload); err != nil {
		s.logger.Printf("HTTP: decode error: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var records []json.RawMessage

	switch v := payload.(type) {
	case []interface{}:
		raw, _ := json.Marshal(v)
		_ = json.Unmarshal(raw, &records)
		s.logger.Printf("HTTP: decoded batch of %d records", len(records))

	case map[string]interface{}:
		raw, _ := json.Marshal(v)
		records = []json.RawMessage{raw}
		s.logger.Println("HTTP: decoded single record")

	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// ---- blocking enqueue (by design) ----
	for i, v := range records {
		rec := &model.Record{
			Key:   nil,
			Value: v,
			Size:  int64(len(v) + len("default")),
		}

		s.logger.Printf("HTTP: enqueue start (record %d)", i)
		s.queue.Enqueue(rec) // BLOCKS if queue full
		s.logger.Printf("HTTP: enqueue done (record %d)", i)
	}

	w.WriteHeader(http.StatusAccepted)
	s.logger.Printf("HTTP: accepted %d records", len(records))
}

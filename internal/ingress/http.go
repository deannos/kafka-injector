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

	mux.Handle("/metrics", promhttp.Handler())

	s := &HTTPServer{
		queue:  q,
		logger: logger,
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	mux.HandleFunc("/ingest", s.handleIngest)
	return s
}

func (s *HTTPServer) Start() error {
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	var records []json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
		var single json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&single); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		records = []json.RawMessage{single}
	}

	for _, v := range records {
		rec := &model.Record{
			Key:   []byte("default"),
			Value: v,
			Size:  int64(len(v) + len("default")),
		}
		s.queue.Enqueue(rec)
	}

	w.WriteHeader(http.StatusAccepted)
}

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

func main() {
	payload := map[string]any{
		"user":  1,
		"event": "click",
		"ts":    time.Now().UnixNano(),
	}

	body, _ := json.Marshal(payload)

	for {
		http.Post(
			"http://localhost:8080/ingest",
			"application/json",
			bytes.NewReader(body),
		)
	}
}

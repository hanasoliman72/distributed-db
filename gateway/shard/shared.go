package shard

// shard.go
//
// Handles all communication from the gateway to individual slaves.
// Every outbound request carries a fresh HMAC token so slaves can verify
// the request really came from the gateway (not a rogue client).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gateway/auth"
	"gateway/metadata"
	"io"
	"log"
	"net/http"
	"time"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Result is the outcome of forwarding one request to one slave.
type Result struct {
	SlaveID    string
	StatusCode int
	Body       map[string]any
	Err        error
}

// Forward sends a JSON payload to slave.URL+endpoint using the given method,
// attaching a fresh HMAC token header.
func Forward(slave *metadata.Slave, method, endpoint string, payload any) Result {
	body, err := json.Marshal(payload)
	if err != nil {
		return Result{SlaveID: slave.ID, Err: fmt.Errorf("shard.Forward: marshal: %w", err)}
	}

	token, err := auth.NewToken()
	if err != nil {
		return Result{SlaveID: slave.ID, Err: fmt.Errorf("shard.Forward: token: %w", err)}
	}

	req, err := http.NewRequest(method, slave.URL+endpoint, bytes.NewReader(body))
	if err != nil {
		return Result{SlaveID: slave.ID, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.HeaderName, token)

	resp, err := httpClient.Do(req)
	if err != nil {
		slave.SetAlive(false)
		log.Printf("[shard] ✗ %s%s: %v", slave.URL, endpoint, err)
		return Result{SlaveID: slave.ID, Err: err}
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var respBody map[string]any
	json.Unmarshal(raw, &respBody)

	if resp.StatusCode >= 500 {
		slave.SetAlive(false)
	}

	log.Printf("[shard] %s %s%s → HTTP %d", method, slave.URL, endpoint, resp.StatusCode)
	return Result{SlaveID: slave.ID, StatusCode: resp.StatusCode, Body: respBody}
}

// ForwardGet sends a GET request to slave.URL+path (path may include query params).
func ForwardGet(slave *metadata.Slave, path string) Result {
	token, err := auth.NewToken()
	if err != nil {
		return Result{SlaveID: slave.ID, Err: err}
	}

	req, err := http.NewRequest("GET", slave.URL+path, nil)
	if err != nil {
		return Result{SlaveID: slave.ID, Err: err}
	}
	req.Header.Set(auth.HeaderName, token)

	resp, err := httpClient.Do(req)
	if err != nil {
		slave.SetAlive(false)
		return Result{SlaveID: slave.ID, Err: err}
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var respBody map[string]any
	json.Unmarshal(raw, &respBody)

	return Result{SlaveID: slave.ID, StatusCode: resp.StatusCode, Body: respBody}
}

// BroadcastAll sends payload to every alive slave and collects results.
// Used for DDL operations (create/drop DB, create/drop table) that must touch
// all shards.
func BroadcastAll(method, endpoint string, payload any) []Result {
	alive := metadata.AliveSlaves()
	if len(alive) == 0 {
		return nil
	}
	ch := make(chan Result, len(alive))
	for _, s := range alive {
		go func(sl *metadata.Slave) { ch <- Forward(sl, method, endpoint, payload) }(s)
	}
	results := make([]Result, 0, len(alive))
	for range alive {
		results = append(results, <-ch)
	}
	close(ch)
	return results
}

package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// ── Slave registry ─────────────────────────────────────────────────────────

// Slave represents one worker node.
type Slave struct {
	URL   string // e.g. "http://localhost:8081"
	Alive bool
	mu    sync.Mutex
}

func (s *Slave) setAlive(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Alive != v {
		if v {
			log.Printf("[replication] slave %s is back ONLINE", s.URL)
		} else {
			log.Printf("[replication] slave %s went OFFLINE", s.URL)
		}
	}
	s.Alive = v
}

func (s *Slave) isAlive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Alive
}

// Registry holds all known slave nodes.
var Registry = []*Slave{
	{URL: "http://localhost:8081", Alive: true}, // Go slave
	{URL: "http://localhost:8082", Alive: true}, // Python slave
	{URL: "http://localhost:8083", Alive: true}, // Node.js slave
}

// ── Broadcast ──────────────────────────────────────────────────────────────

// Broadcast sends payload as JSON POST to endpoint on every alive slave.
// Called with `go Broadcast(...)` so it never blocks the HTTP handler.
func Broadcast(endpoint string, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[replication] marshal error: %v", err)
		return
	}

	var wg sync.WaitGroup
	for _, slave := range Registry {
		if !slave.isAlive() {
			continue
		}
		wg.Add(1)
		go func(s *Slave) {
			defer wg.Done()
			sendToSlave(s, endpoint, body)
		}(slave)
	}
	wg.Wait()
}

// sendToSlave posts body to slave.URL+endpoint.
// If the request fails the slave is marked offline.
func sendToSlave(s *Slave, endpoint string, body []byte) {
	url := s.URL + endpoint
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[replication] POST %s failed: %v", url, err)
		s.setAlive(false)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		log.Printf("[replication] POST %s returned HTTP %d", url, resp.StatusCode)
	}
}

// ── Health checker ─────────────────────────────────────────────────────────

// StartHealthChecker pings every slave every `interval` seconds.
// When a slave comes back online, it is re-synced with current data.
func StartHealthChecker(interval time.Duration, snapshotFn func() map[string]any) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			for _, slave := range Registry {
				wasAlive := slave.isAlive()
				alive := ping(slave.URL)
				slave.setAlive(alive)

				// Slave just came back – push a full snapshot.
				if !wasAlive && alive && snapshotFn != nil {
					go pushSnapshot(slave, snapshotFn())
				}
			}
		}
	}()
}

// ping does a GET /health and returns true on HTTP 200.
func ping(baseURL string) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ── Full-snapshot sync ─────────────────────────────────────────────────────

// pushSnapshot sends the full current state to a slave that just recovered.
// The snapshot is produced by main.go via BuildSnapshot().
func pushSnapshot(s *Slave, snapshot map[string]any) {
	body, err := json.Marshal(snapshot)
	if err != nil {
		log.Printf("[replication] snapshot marshal error: %v", err)
		return
	}

	url := s.URL + "/replicate/snapshot"
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[replication] snapshot push to %s failed: %v", s.URL, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[replication] snapshot pushed to %s → HTTP %d", s.URL, resp.StatusCode)
}

// ── Status endpoint helper ─────────────────────────────────────────────────

// SlaveStatus is used by the /replication/status handler.
type SlaveStatus struct {
	URL   string `json:"url"`
	Alive bool   `json:"alive"`
}

// Status returns the current liveness of every registered slave.
func Status() []SlaveStatus {
	out := make([]SlaveStatus, len(Registry))
	for i, s := range Registry {
		out[i] = SlaveStatus{URL: s.URL, Alive: s.isAlive()}
	}
	return out
}

// AddSlave registers a new slave at runtime (used by /replication/add).
func AddSlave(url string) {
	for _, s := range Registry {
		if s.URL == url {
			fmt.Printf("[replication] slave %s already registered\n", url)
			return
		}
	}
	Registry = append(Registry, &Slave{URL: url, Alive: true})
	log.Printf("[replication] new slave registered: %s", url)
}

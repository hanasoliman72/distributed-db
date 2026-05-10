package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════
//  CHANNEL ARCHITECTURE OVERVIEW
//
//  1. Slave.stateCh   – every read/write of Slave.alive goes through a
//                       dedicated goroutine; no mutex anywhere.
//
//  2. Broadcast()     – fires one goroutine per slave, collects results
//                       through a buffered channel (no WaitGroup).
//
//  3. pushSnapshot()  – one goroutine + one buffered channel for the result.
//
//  4. StartHealthChecker() – ping goroutines send healthEvent structs into
//                       an unbuffered eventCh; a consumer goroutine reacts.
//
//  5. WriteQueue      – HTTP handlers push WriteJob structs here; a single
//                       worker goroutine drains the queue serially and sends
//                       the outcome back through each job's own ResultCh.
// ═══════════════════════════════════════════════════════════════════════════

// ── Slave state (channel-based, zero mutexes) ─────────────────────────────

// slaveStateMsg is the message type sent through Slave.stateCh.
// If it carries a responseCh it is a read request; otherwise a write.
type slaveStateMsg struct {
	newValue   bool      // used for writes
	isWrite    bool      // true → write, false → read
	responseCh chan bool // non-nil for reads; worker sends current value here
}

// Slave represents one worker node.
// All access to `alive` is serialised through stateCh – no sync.Mutex.
type Slave struct {
	URL     string
	stateCh chan slaveStateMsg
}

// newSlave creates a Slave and starts its state-manager goroutine.
func newSlave(url string, initialAlive bool) *Slave {
	s := &Slave{
		URL:     url,
		stateCh: make(chan slaveStateMsg, 8),
	}
	// State-manager goroutine: owns `alive`, serialises all reads and writes.
	go func() {
		alive := initialAlive
		for msg := range s.stateCh {
			if msg.isWrite {
				if alive != msg.newValue {
					if msg.newValue {
						log.Printf("[replication] slave %s is back ONLINE", s.URL)
					} else {
						log.Printf("[replication] slave %s went OFFLINE", s.URL)
					}
					alive = msg.newValue
				}
			} else {
				// Read request – send current value back.
				msg.responseCh <- alive
			}
		}
	}()
	return s
}

func (s *Slave) setAlive(v bool) {
	s.stateCh <- slaveStateMsg{newValue: v, isWrite: true}
}

func (s *Slave) isAlive() bool {
	responseCh := make(chan bool, 1)
	s.stateCh <- slaveStateMsg{isWrite: false, responseCh: responseCh}
	return <-responseCh
}

// ── Slave registry ────────────────────────────────────────────────────────

var Registry = []*Slave{
	newSlave("http://192.168.16.9:8080", true), // Go slave      – port 8081
	newSlave("http://192.168.16.11:8082", true), // Python slave  – port 8082
	//newSlave("http://192.168.16.9:8080", true), // C# slave      – port 8080 (change IP if on another PC)
}

// ── Broadcast result ──────────────────────────────────────────────────────

// BroadcastResult carries the outcome of one slave replication attempt.
type BroadcastResult struct {
	SlaveURL string `json:"slave_url"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
}

// ── Broadcast (pure channel, no WaitGroup) ────────────────────────────────

// Broadcast sends payload as JSON POST to endpoint on every alive slave.
// One goroutine per slave; results are collected from a buffered channel.
// Call with `go Broadcast(...)` from handlers to avoid blocking responses.
func Broadcast(endpoint string, payload any) []BroadcastResult {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[replication] marshal error: %v", err)
		return nil
	}

	// Build list of alive targets first so we know the exact buffer size.
	targets := make([]*Slave, 0, len(Registry))
	for _, s := range Registry {
		if s.isAlive() {
			targets = append(targets, s)
		}
	}
	if len(targets) == 0 {
		log.Println("[replication] no alive slaves – skipping broadcast")
		return nil
	}

	// Buffered channel: every goroutine can send without blocking.
	resultCh := make(chan BroadcastResult, len(targets))

	for _, slave := range targets {
		go func(s *Slave) {
			resultCh <- sendToSlave(s, endpoint, body)
		}(slave)
	}

	// Collect exactly len(targets) results then close.
	results := make([]BroadcastResult, 0, len(targets))
	for range targets {
		r := <-resultCh
		results = append(results, r)
		if r.Success {
			log.Printf("[replication] ✓ %s%s", r.SlaveURL, endpoint)
		} else {
			log.Printf("[replication] ✗ %s%s  err=%s", r.SlaveURL, endpoint, r.Error)
		}
	}
	close(resultCh)
	return results
}

// sendToSlave POSTs body to s.URL+endpoint and returns a BroadcastResult.
func sendToSlave(s *Slave, endpoint string, body []byte) BroadcastResult {
	url := s.URL + endpoint
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		s.setAlive(false)
		return BroadcastResult{SlaveURL: s.URL, Success: false, Error: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return BroadcastResult{
			SlaveURL: s.URL,
			Success:  false,
			Error:    fmt.Sprintf("HTTP %d", resp.StatusCode),
		}
	}
	return BroadcastResult{SlaveURL: s.URL, Success: true}
}

// ── Health checker (channel-based) ───────────────────────────────────────

// healthEvent is produced by ping goroutines and consumed by the event loop.
type healthEvent struct {
	slave    *Slave
	cameBack bool // true = was dead, now alive
}

// StartHealthChecker pings every slave every `interval`.
// Ping goroutines → eventCh → consumer goroutine (no mutex, no WaitGroup).
func StartHealthChecker(interval time.Duration, snapshotFn func() map[string]any) {
	// Unbuffered: consumer must be ready before any ping goroutine can send.
	eventCh := make(chan healthEvent)

	// ── Consumer goroutine ───────────────────────────────────────────────
	go func() {
		for ev := range eventCh {
			if ev.cameBack && snapshotFn != nil {
				// Push full snapshot to the recovered slave in its own goroutine.
				go pushSnapshot(ev.slave, snapshotFn())
			}
		}
	}()

	// ── Ticker goroutine ─────────────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			for _, slave := range Registry {
				go func(s *Slave) {
					wasAlive := s.isAlive()
					nowAlive := ping(s.URL)
					s.setAlive(nowAlive)
					// Only send an event when liveness actually changed.
					switch {
					case !wasAlive && nowAlive:
						eventCh <- healthEvent{slave: s, cameBack: true}
					case wasAlive && !nowAlive:
						eventCh <- healthEvent{slave: s, cameBack: false}
					}
				}(slave)
			}
		}
	}()
}

// ping does GET /health and returns true on HTTP 200.
func ping(baseURL string) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ── Snapshot push (channel-based) ────────────────────────────────────────

// pushSnapshot sends the full current state to a slave that just recovered.
// Uses a buffered channel to collect the result from its own goroutine.
func pushSnapshot(s *Slave, snapshot map[string]any) {
	body, err := json.Marshal(snapshot)
	if err != nil {
		log.Printf("[replication] snapshot marshal error: %v", err)
		return
	}

	resultCh := make(chan BroadcastResult, 1)
	go func() {
		resultCh <- sendToSlave(s, "/replicate/snapshot", body)
	}()

	result := <-resultCh
	close(resultCh)

	if result.Success {
		log.Printf("[replication] snapshot → %s ✓", s.URL)
	} else {
		log.Printf("[replication] snapshot → %s ✗  err=%s", s.URL, result.Error)
	}
}

// ── Write queue (channel-based serialised writes) ─────────────────────────

// WriteJob is pushed by HTTP handlers into WriteQueue.
// The handler blocks on ResultCh until the worker finishes – serialising
// all writes without any mutex on the handler side.
type WriteJob struct {
	Operation string // "insert" | "update" | "delete"
	DB        string
	Table     string
	Payload   map[string]any // insert
	Where     map[string]any // update / delete
	Set       map[string]any // update
	ResultCh  chan error     // worker sends outcome back here
}

// WriteQueue is the global write channel. Buffer of 100 lets handlers
// queue without blocking unless there is a genuine backlog.
var WriteQueue = make(chan WriteJob, 100)

// StartWriteWorker launches the single goroutine that drains WriteQueue.
// Pass in the three storage functions so the replication package stays
// decoupled from the storage package.
func StartWriteWorker(
	insertFn func(db, table string, record map[string]any) (int64, error),
	updateFn func(db, table string, where, set map[string]any) (int, error),
	deleteFn func(db, table string, where map[string]any) (int, error),
) {
	go func() {
		for job := range WriteQueue {
			var err error
			switch job.Operation {
			case "insert":
				_, err = insertFn(job.DB, job.Table, job.Payload)
			case "update":
				_, err = updateFn(job.DB, job.Table, job.Where, job.Set)
			case "delete":
				_, err = deleteFn(job.DB, job.Table, job.Where)
			default:
				err = fmt.Errorf("unknown operation: %s", job.Operation)
			}
			job.ResultCh <- err // unblock the waiting HTTP handler
		}
	}()
}

// ── Status & registry helpers ─────────────────────────────────────────────

// SlaveStatus is returned by the /replication/status endpoint.
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

// AddSlave registers a new slave at runtime (/replication/add endpoint).
func AddSlave(url string) {
	for _, s := range Registry {
		if s.URL == url {
			log.Printf("[replication] slave %s already registered", url)
			return
		}
	}
	Registry = append(Registry, newSlave(url, true))
	log.Printf("[replication] new slave registered: %s", url)
}

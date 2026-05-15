package main

// gateway/main.go
//
// API Gateway — the single entry point for all client requests.
// Responsibilities:
//   - Maintain metadata (shard map, table schema list)
//   - Route DML requests to the correct shard slave
//   - Broadcast DDL to all slaves
//   - Sign every outbound request with an HMAC token
//   - Health-check slaves and mark them offline on failure
//   - Persist metadata to metadata.json after every DDL change
//   - Expose /gateway/status and /gateway/promote for debugging and failover

import (
	"encoding/json"
	"gateway/auth"
	"gateway/handlers"
	"gateway/metadata"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	defaultPort   = ":8080"
	gatewaySecret = "Hana-1234"
	slaveAURL     = "http://127.0.0.1:8081"
	slaveBURL     = "http://127.0.0.1:8082"
	slaveCURL     = "http://127.0.0.1:8083"
)

func main() {
	// ── 1. Configure auth secret ────────────────────────────────────────
	auth.SetSecret(gatewaySecret)
	log.Println("[gateway] HMAC secret loaded")

	// ── 2. Register slaves ───────────────────────────────────────────────
	metadata.RegisterSlave("slave-a", slaveAURL)
	metadata.RegisterSlave("slave-b", slaveBURL)
	metadata.RegisterSlave("slave-c", slaveCURL)
	log.Printf("[gateway] slaves: %s  %s  %s", slaveAURL, slaveBURL, slaveCURL)

	// ── 5. Start health checker ──────────────────────────────────────────
	metadata.StartHealthChecker(10 * time.Second)
	log.Println("[gateway] health checker started (interval=10s)")

	// ── 6. Build mux and listen ──────────────────────────────────────────
	mux := buildMux()
	log.Printf("[gateway] listening on %s", defaultPort)
	log.Printf("number of slaves: %d", len(metadata.Registry))
	if err := http.ListenAndServe(defaultPort, mux); err != nil {
		log.Fatal(err)
	}
}

// buildMux creates and returns the ServeMux with all routes registered.
// Kept as a separate function so it is easy to unit-test handlers.
func buildMux() *http.ServeMux {
	mux := http.NewServeMux()

	// DDL
	mux.HandleFunc("/db/create", method("POST", handlers.CreateDB))
	mux.HandleFunc("/db/drop", method("DELETE", handlers.DropDB))
	mux.HandleFunc("/table/create", method("POST", handlers.CreateTable))
	mux.HandleFunc("/table/drop", method("DELETE", handlers.DropTable))

	// DML
	mux.HandleFunc("/query/insert", method("POST", handlers.Insert))
	mux.HandleFunc("/query/select", method("GET", handlers.Select))
	mux.HandleFunc("/query/update", method("PUT", handlers.Update))
	mux.HandleFunc("/query/delete", method("DELETE", handlers.Delete))
	mux.HandleFunc("/query/search", method("GET", handlers.Search))

	// Health — no auth required (slave-go's watcher and load balancers hit this)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "role": "gateway"})
	})

	// Status
	mux.HandleFunc("/gateway/status", func(w http.ResponseWriter, r *http.Request) {
		type slaveInfo struct {
			ID    string `json:"id"`
			URL   string `json:"url"`
			Alive bool   `json:"alive"`
		}
		slaves := make([]slaveInfo, len(metadata.Registry))
		for i, s := range metadata.Registry {
			slaves[i] = slaveInfo{ID: s.ID, URL: s.URL, Alive: s.IsAlive()}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"slaves": slaves})
	})

	// /gateway/promote — slave-go POSTs here to announce it has taken over.
	// If the original gateway somehow recovers it will see this notice in its
	// log and can be restarted as a standby.
	mux.HandleFunc("/gateway/promote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			SlaveID string `json:"slave_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		log.Printf("[gateway] PROMOTE notice received from slave %s", body.SlaveID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "acknowledged"})
	})

	// /gateway/sync — promoted slave POSTs its metadata.json to restore master
	mux.HandleFunc("/gateway/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		if err := metadata.LoadMetadataFromBytes(data); err != nil {
			log.Printf("[gateway] sync: invalid metadata payload: %v", err)
			http.Error(w, "invalid metadata payload", http.StatusBadRequest)
			return
		}
		if err := os.WriteFile(metadata.MetadataFile, data, 0644); err != nil {
			log.Printf("[gateway] sync: write error: %v", err)
			http.Error(w, "failed to write metadata", http.StatusInternalServerError)
			return
		}
		log.Printf("[gateway] metadata synced from promoted slave and loaded")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "metadata synced"})
	})

	return mux
}

func method(m string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != m {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

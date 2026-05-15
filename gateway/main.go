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
//   - Expose /gateway/status for debugging

import (
	"encoding/json"
	"gateway/auth"
	"gateway/handlers"
	"gateway/metadata"
	"log"
	"net/http"
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
	log.Println("[gateway] HMAC secret loaded from constant gatewaySecret")

	// ── 2. Register slaves ───────────────────────────────────────────────
	metadata.RegisterSlave("slave-a", slaveAURL)
	metadata.RegisterSlave("slave-b", slaveBURL)
	metadata.RegisterSlave("slave-c", slaveCURL)
	log.Printf("[gateway] slaves: %s  %s  %s", slaveAURL, slaveBURL, slaveCURL)

	// ── 3. Start health checker ──────────────────────────────────────────
	metadata.StartHealthChecker(10 * time.Second)
	log.Println("[gateway] health checker started (interval=10s)")

	// ── 4. Register routes ───────────────────────────────────────────────
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

	// Health / status
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "role": "gateway"})
	})

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

	// ── 5. Start HTTP server ─────────────────────────────────────────────
	log.Printf("[gateway] listening on %s", defaultPort)
	if err := http.ListenAndServe(defaultPort, mux); err != nil {
		log.Fatal(err)
	}
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

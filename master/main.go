package main

import (
	"encoding/json"
	"log"
	"master/handlers"
	"master/replication"
	"master/storage"
	"net/http"
	"time"
)

// ── MySQL connection config ───────────────────────────────────────────────
// Change these to match your MySQL setup.
const (
	mysqlUser     = "root"
	mysqlPassword = "rootroot"
	mysqlHost     = "127.0.0.1"
	mysqlPort     = "3306"
)

func main() {
	// ── 1. Connect to MySQL ──────────────────────────────────────────────
	dsn := mysqlUser + ":" + mysqlPassword + "@tcp(" + mysqlHost + ":" + mysqlPort + ")/"
	if err := storage.Connect(dsn); err != nil {
		log.Fatal("Cannot connect to MySQL:", err)
	}
	log.Println("Connected to MySQL at", mysqlHost+":"+mysqlPort)

	// ── 2. Start the write-queue worker goroutine ────────────────────────
	// All INSERT / UPDATE / DELETE requests are serialised through the
	// WriteQueue channel.  No mutex needed on the handler side.
	replication.StartWriteWorker(
		storage.InsertRecord,
		storage.UpdateRecords,
		storage.DeleteRecords,
	)
	log.Println("Write-queue worker started")

	// ── 3. Start the health checker ──────────────────────────────────────
	// Pings slaves every 10 s; pushes a full snapshot when one recovers.
	replication.StartHealthChecker(10*time.Second, buildSnapshot)
	log.Println("Health checker started (interval=10s)")

	// ── 4. Register HTTP routes ──────────────────────────────────────────
	mux := http.NewServeMux()

	// DB
	mux.HandleFunc("/db/create", method("POST", handlers.CreateDB))
	mux.HandleFunc("/db/drop", method("DELETE", handlers.DropDB))
	mux.HandleFunc("/db/list", method("GET", handlers.ListDBs))

	// Table
	mux.HandleFunc("/table/create", method("POST", handlers.CreateTable))
	mux.HandleFunc("/table/drop", method("DELETE", handlers.DropTable))
	mux.HandleFunc("/table/list", method("GET", handlers.ListTables))

	// Query
	mux.HandleFunc("/query/insert", method("POST", handlers.Insert))
	mux.HandleFunc("/query/select", method("GET", handlers.Select))
	mux.HandleFunc("/query/update", method("PUT", handlers.Update))
	mux.HandleFunc("/query/delete", method("DELETE", handlers.Delete))

	// Health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "role": "master"})
	})

	// Replication management
	mux.HandleFunc("/replication/status", replicationStatus)
	mux.HandleFunc("/replication/add", replicationAdd)

	// ── 5. Start HTTP server ─────────────────────────────────────────────
	log.Println("Master node listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

// method restricts a handler to one HTTP method.
func method(m string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != m {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// GET /replication/status
func replicationStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"slaves": replication.Status()})
}

// POST /replication/add  body: { "url": "http://localhost:8084" }
func replicationAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"field 'url' is required"}`, http.StatusBadRequest)
		return
	}
	replication.AddSlave(req.URL)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "slave registered: " + req.URL})
}

// buildSnapshot reads the full state from MySQL and returns it as a map.
// Sent to slaves that recover after being offline.
func buildSnapshot() map[string]any {
	dbList, err := storage.ListDBs()
	if err != nil {
		log.Println("[snapshot] ListDBs error:", err)
		return nil
	}
	databases := map[string]any{}
	for _, db := range dbList {
		tables, err := storage.ListTables(db)
		if err != nil {
			continue
		}
		tableMap := map[string]any{}
		for _, tbl := range tables {
			snap, err := storage.GetFullTable(db, tbl)
			if err != nil {
				continue
			}
			tableMap[tbl] = map[string]any{
				"attributes": snap.Attributes,
				"records":    snap.Records,
			}
		}
		databases[db] = tableMap
	}
	return map[string]any{"databases": databases}
}

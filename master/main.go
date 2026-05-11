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

// ── config ───────────────────────────────────────────────
const (
	mysqlUser     = "root"
	mysqlPassword = "root"
	mysqlHost     = "127.0.0.1"
	mysqlPort     = ":3306"
	masterPort    = ":8080"
)

func main() {
	// ── 1. Connect to MySQL ──────────────────────────────────────────────
	dsn := mysqlUser + ":" + mysqlPassword + "@tcp(" + mysqlHost + mysqlPort + ")/"
	if err := storage.Connect(dsn); err != nil {
		log.Fatal("Cannot connect to MySQL:", err)
	}
	log.Println("Connected to MySQL at", mysqlHost+mysqlPort)

	// ── 2. Start the write-queue worker goroutine ────────────────────────
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

	// Table
	mux.HandleFunc("/table/create", method("POST", handlers.CreateTable))
	mux.HandleFunc("/table/drop", method("DELETE", handlers.DropTable))

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
	mux.HandleFunc("/replicate/query/insert", method("POST", replication.ReceiveInsert))
	mux.HandleFunc("/replicate/query/update", method("POST", replication.ReceiveUpdate))
	mux.HandleFunc("/replicate/query/delete", method("POST", replication.ReceiveDelete))
	mux.HandleFunc("/replicate/table/drop", method("POST", replication.ReceiveDropTable))

	// Slaves GET this on startup or after recovery to pull a full snapshot.
	mux.HandleFunc("/snapshot", method("GET", replication.ServeSnapshot(buildSnapshot)))

	// ── 5. Start HTTP server ─────────────────────────────────────────────
	log.Println("Master node listening on ", masterPort)
	if err := http.ListenAndServe(masterPort, mux); err != nil {
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

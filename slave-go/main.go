package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

// ── Simple local storage (mirrors master's JSON format) ──────────────────

var mu sync.Mutex

const dataDir = "data"

func dbPath(db string) string           { return filepath.Join(dataDir, db) }
func tablePath(db, table string) string { return filepath.Join(dataDir, db, table+".json") }

type tableFile struct {
	Meta struct {
		Attributes []string `json:"attributes"`
	} `json:"meta"`
	Records []map[string]any `json:"records"`
}

func readTable(db, table string) (*tableFile, error) {
	raw, err := os.ReadFile(tablePath(db, table))
	if err != nil {
		return nil, err
	}
	var tf tableFile
	return &tf, json.Unmarshal(raw, &tf)
}

func writeTable(db, table string, tf *tableFile) error {
	if err := os.MkdirAll(dbPath(db), 0755); err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(tf, "", "  ")
	return os.WriteFile(tablePath(db, table), raw, 0644)
}

func matches(record, where map[string]any) bool {
	for k, v := range where {
		rv, ok := record[k]
		if !ok || fmt.Sprintf("%v", rv) != fmt.Sprintf("%v", v) {
			return false
		}
	}
	return true
}

// ── Helpers ───────────────────────────────────────────────────────────────

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func decode(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

// ── Health ────────────────────────────────────────────────────────────────

func healthHandler(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, map[string]string{"status": "ok", "role": "slave-go"})
}

// ── Replication receivers ─────────────────────────────────────────────────

// POST /replicate/table/create
func replicateCreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	defer mu.Unlock()
	tf := &tableFile{}
	tf.Meta.Attributes = req.Attributes
	tf.Records = []map[string]any{}
	if err := writeTable(req.DB, req.Table, tf); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

// POST /replicate/table/drop
func replicateDropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	defer mu.Unlock()
	os.Remove(tablePath(req.DB, req.Table))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

// POST /replicate/query/insert
func replicateInsert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	defer mu.Unlock()
	tf, err := readTable(req.DB, req.Table)
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	tf.Records = append(tf.Records, req.Record)
	writeTable(req.DB, req.Table, tf)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

// POST /replicate/query/update
func replicateUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	defer mu.Unlock()
	tf, err := readTable(req.DB, req.Table)
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	for i, rec := range tf.Records {
		if matches(rec, req.Where) {
			for k, v := range req.Set {
				tf.Records[i][k] = v
			}
		}
	}
	writeTable(req.DB, req.Table, tf)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

// POST /replicate/query/delete
func replicateDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	defer mu.Unlock()
	tf, err := readTable(req.DB, req.Table)
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	var kept []map[string]any
	for _, rec := range tf.Records {
		if !matches(rec, req.Where) {
			kept = append(kept, rec)
		}
	}
	tf.Records = kept
	writeTable(req.DB, req.Table, tf)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

// POST /replicate/snapshot  – full state sync when slave recovers
func replicateSnapshot(w http.ResponseWriter, r *http.Request) {
	var snapshot struct {
		Databases map[string]map[string]struct {
			Attributes []string         `json:"attributes"`
			Records    []map[string]any `json:"records"`
		} `json:"databases"`
	}
	if err := decode(r, &snapshot); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	defer mu.Unlock()
	// Wipe and rebuild everything
	os.RemoveAll(dataDir)
	for dbName, tables := range snapshot.Databases {
		for tblName, tbl := range tables {
			tf := &tableFile{}
			tf.Meta.Attributes = tbl.Attributes
			tf.Records = tbl.Records
			writeTable(dbName, tblName, tf)
		}
	}
	respond(w, http.StatusOK, map[string]string{"status": "snapshot applied"})
}

// ── Local read query (slaves can serve SELECTs independently) ────────────

// POST /query/select
func localSelect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mu.Lock()
	tf, err := readTable(req.DB, req.Table)
	mu.Unlock()
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	var result []map[string]any
	for _, rec := range tf.Records {
		if len(req.Where) == 0 || matches(rec, req.Where) {
			result = append(result, rec)
		}
	}
	if result == nil {
		result = []map[string]any{}
	}
	respond(w, http.StatusOK, map[string]any{"count": len(result), "records": result, "served_by": "slave-go"})
}

// ── Main ─────────────────────────────────────────────────────────────────

func main() {
	os.MkdirAll(dataDir, 0755)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)

	// Replication endpoints
	mux.HandleFunc("/replicate/table/create", replicateCreateTable)
	mux.HandleFunc("/replicate/table/drop", replicateDropTable)
	mux.HandleFunc("/replicate/query/insert", replicateInsert)
	mux.HandleFunc("/replicate/query/update", replicateUpdate)
	mux.HandleFunc("/replicate/query/delete", replicateDelete)
	mux.HandleFunc("/replicate/snapshot", replicateSnapshot)

	// Local query (read-only in real scenario; here we expose select)
	mux.HandleFunc("/query/select", localSelect)

	port := ":8081"
	log.Printf("Go slave listening on %s", port)
	log.Fatal(http.ListenAndServe(port, mux))
}

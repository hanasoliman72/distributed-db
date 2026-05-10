package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ── Config ────────────────────────────────────────────────────────────────

const (
	mysqlUser     = "root"
	mysqlPassword = "root"
	mysqlHost     = "127.0.0.1"
	mysqlPort     = "3306"

	selfAddr = "http://127.0.0.1:8080"
)

// Cluster peers: master first, then all other slaves.
// The broadcaster skips selfAddr automatically.
var peers = []string{
	"http://127.0.0.1:8080", // master
	"http://127.0.0.1:8081", // C# slave
	"http://127.0.0.1:8082", // Go slave
	"http://127.0.0.1:8083", // Python slave
}

// ── Fault-tolerance state ─────────────────────────────────────────────────

var (
	masterAddr   = peers[0]
	masterDown   bool
	masterDownMu sync.RWMutex
	isSelfMaster bool // true when this slave promoted itself
	selfRole     = "slave-go"
)

func setMasterDown(down bool) {
	masterDownMu.Lock()
	defer masterDownMu.Unlock()
	if down && !masterDown {
		log.Printf("[fault] master %s is unreachable — promoting self to acting master", masterAddr)
		isSelfMaster = true
		selfRole = "slave-go (acting master)"
	} else if !down && masterDown {
		log.Printf("[fault] master %s is back online — reverting to slave role", masterAddr)
		isSelfMaster = false
		selfRole = "slave-go"
	}
	masterDown = down
}

func isMasterDown() bool {
	masterDownMu.RLock()
	defer masterDownMu.RUnlock()
	return masterDown
}

// masterWatcher pings the master every 5 s so we can detect recovery.
func masterWatcher() {
	for {
		time.Sleep(5 * time.Second)
		_, err := http.Get(masterAddr + "/health")
		if err != nil {
			setMasterDown(true)
		} else {
			setMasterDown(false)
		}
	}
}

// ── MySQL ─────────────────────────────────────────────────────────────────

var db *sql.DB

func connectMySQL() error {
	dsn := mysqlUser + ":" + mysqlPassword + "@tcp(" + mysqlHost + ":" + mysqlPort + ")/"
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	return db.Ping()
}

// ── Broadcaster ───────────────────────────────────────────────────────────
// Sends a POST to every peer except ourselves.
// If a peer is the master and it's currently marked down, we skip it (not
// strictly required, but avoids stacking timeouts).

func broadcast(path string, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[broadcast] marshal error: %v", err)
		return
	}
	for _, peer := range peers {
		if peer == selfAddr {
			continue // skip ourselves
		}
		if peer == masterAddr && isMasterDown() {
			log.Printf("[broadcast] skipping down master %s", peer)
			continue
		}
		go func(url string) {
			resp, err := http.Post(url+path, "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf("[broadcast] POST %s%s failed: %v", url, path, err)
				if url == masterAddr {
					setMasterDown(true)
				}
				return
			}
			resp.Body.Close()
			if url == masterAddr {
				setMasterDown(false) // successful contact → mark master alive
			}
		}(peer)
	}
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

func buildWhere(where map[string]any) (string, []any) {
	clauses := make([]string, 0, len(where))
	args := make([]any, 0, len(where))
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	return strings.Join(clauses, " AND "), args
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []map[string]any
	for rows.Next() {
		valuePtrs := make([]interface{}, len(cols))
		values := make([]interface{}, len(cols))
		for i := range valuePtrs {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if b, ok := values[i].([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = values[i]
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// ── Health ────────────────────────────────────────────────────────────────

func healthHandler(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, map[string]string{
		"status": "ok",
		"role":   selfRole,
	})
}

// ── Replication receivers (called by master OR other slaves) ──────────────
// These are unchanged from the original — they just apply whatever they
// receive to local MySQL. The broadcaster in each write handler is what
// adds the bidirectional fan-out.

func replicateCreateDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db required"})
		return
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	log.Printf("[slave] DB '%s' created via replication", req.DB)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateDropDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db required"})
		return
	}
	if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", req.DB)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	log.Printf("[slave] DB '%s' dropped via replication", req.DB)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

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
	db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB))
	colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, a := range req.Attributes {
		if strings.ToLower(a) != "id" {
			colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
		}
	}
	if _, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		req.DB, req.Table, strings.Join(colDefs, ", "),
	)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateDropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", req.DB, req.Table))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

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
	cols := make([]string, 0, len(req.Record))
	placeholders := make([]string, 0, len(req.Record))
	values := make([]any, 0, len(req.Record))
	for col, val := range req.Record {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, fmt.Sprintf("%v", val))
	}
	if _, err := db.Exec(fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		req.DB, req.Table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	), values...); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

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
	setClauses := make([]string, 0, len(req.Set))
	args := []any{}
	for col, val := range req.Set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s", req.DB, req.Table, strings.Join(setClauses, ", "))
	if len(req.Where) > 0 {
		cond, whereArgs := buildWhere(req.Where)
		query += " WHERE " + cond
		args = append(args, whereArgs...)
	}
	if _, err := db.Exec(query, args...); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

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
	query := fmt.Sprintf("DELETE FROM `%s`.`%s`", req.DB, req.Table)
	args := []any{}
	if len(req.Where) > 0 {
		cond, whereArgs := buildWhere(req.Where)
		query += " WHERE " + cond
		args = whereArgs
	}
	if _, err := db.Exec(query, args...); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

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
	for dbName, tables := range snapshot.Databases {
		db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
		for tblName, tbl := range tables {
			db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, tblName))
			colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
			for _, a := range tbl.Attributes {
				if strings.ToLower(a) != "id" {
					colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
				}
			}
			db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)", dbName, tblName, strings.Join(colDefs, ", ")))
			for _, rec := range tbl.Records {
				cols := make([]string, 0)
				placeholders := make([]string, 0)
				vals := make([]any, 0)
				for col, val := range rec {
					cols = append(cols, fmt.Sprintf("`%s`", col))
					placeholders = append(placeholders, "?")
					vals = append(vals, fmt.Sprintf("%v", val))
				}
				db.Exec(fmt.Sprintf(
					"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
					dbName, tblName,
					strings.Join(cols, ", "),
					strings.Join(placeholders, ", "),
				), vals...)
			}
		}
	}
	respond(w, http.StatusOK, map[string]string{"status": "snapshot applied"})
}

// ── SELECT (read-only) ────────────────────────────────────────────────────

func localSelect(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dbName := q.Get("db")
	table := q.Get("table")
	if dbName == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' and 'table' are required"})
		return
	}
	where := map[string]any{}
	for key, vals := range q {
		if key != "db" && key != "table" {
			where[key] = vals[0]
		}
	}
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", dbName, table)
	args := []any{}
	if len(where) > 0 {
		cond, whereArgs := buildWhere(where)
		query += " WHERE " + cond
		args = whereArgs
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()
	records, err := scanRows(rows)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if records == nil {
		records = []map[string]any{}
	}
	respond(w, http.StatusOK, map[string]any{
		"count":     len(records),
		"records":   records,
		"served_by": selfRole + " :8082",
	})
}

// ── LOCAL WRITE ENDPOINTS (bidirectional: apply locally + broadcast) ──────

// POST /query/insert
func localInsert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'record' are required"})
		return
	}
	cols := make([]string, 0, len(req.Record))
	placeholders := make([]string, 0, len(req.Record))
	values := make([]any, 0, len(req.Record))
	for col, val := range req.Record {
		if strings.ToLower(col) == "id" {
			continue
		}
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, fmt.Sprintf("%v", val))
	}
	result, err := db.Exec(fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		req.DB, req.Table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	), values...)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	generatedID, _ := result.LastInsertId()

	// Include the generated id so all peers stay in sync on the same id.
	broadcastRecord := make(map[string]any, len(req.Record)+1)
	for k, v := range req.Record {
		broadcastRecord[k] = v
	}
	broadcastRecord["id"] = generatedID
	broadcast("/replicate/query/insert", map[string]any{
		"db":     req.DB,
		"table":  req.Table,
		"record": broadcastRecord,
	})

	respond(w, http.StatusCreated, map[string]any{
		"message":      "record inserted",
		"generated_id": generatedID,
		"served_by":    selfRole + " :8081",
	})
}

// PUT /query/update
func localUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Set == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', 'where', and 'set' are required"})
		return
	}
	setClauses := make([]string, 0, len(req.Set))
	args := []any{}
	for col, val := range req.Set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s", req.DB, req.Table, strings.Join(setClauses, ", "))
	if len(req.Where) > 0 {
		cond, whereArgs := buildWhere(req.Where)
		query += " WHERE " + cond
		args = append(args, whereArgs...)
	}
	result, err := db.Exec(query, args...)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	affected, _ := result.RowsAffected()

	broadcast("/replicate/query/update", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
		"set":   req.Set,
	})

	respond(w, http.StatusOK, map[string]any{
		"message":         "update complete",
		"records_updated": affected,
		"served_by":       selfRole + " :8081",
	})
}

// DELETE /query/delete
func localDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'where' are required"})
		return
	}
	query := fmt.Sprintf("DELETE FROM `%s`.`%s`", req.DB, req.Table)
	args := []any{}
	if len(req.Where) > 0 {
		cond, whereArgs := buildWhere(req.Where)
		query += " WHERE " + cond
		args = whereArgs
	}
	result, err := db.Exec(query, args...)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	affected, _ := result.RowsAffected()

	broadcast("/replicate/query/delete", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
	})

	respond(w, http.StatusOK, map[string]any{
		"message":         "delete complete",
		"records_deleted": affected,
		"served_by":       selfRole + " :8081",
	})
}

// ── Main ──────────────────────────────────────────────────────────────────

func main() {
	if err := connectMySQL(); err != nil {
		log.Fatal("Cannot connect to MySQL:", err)
	}
	log.Println("Go slave connected to MySQL")

	// Start background master health watcher
	go masterWatcher()

	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/health", healthHandler)

	// Replication receivers (called by master OR other slaves)
	mux.HandleFunc("/replicate/db/create", replicateCreateDB)
	mux.HandleFunc("/replicate/db/drop", replicateDropDB)
	mux.HandleFunc("/replicate/table/create", replicateCreateTable)
	mux.HandleFunc("/replicate/table/drop", replicateDropTable)
	mux.HandleFunc("/replicate/query/insert", replicateInsert)
	mux.HandleFunc("/replicate/query/update", replicateUpdate)
	mux.HandleFunc("/replicate/query/delete", replicateDelete)
	mux.HandleFunc("/replicate/snapshot", replicateSnapshot)

	// Client-facing endpoints (apply locally + broadcast to all peers)
	mux.HandleFunc("/query/select", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		localSelect(w, r)
	})
	mux.HandleFunc("/query/insert", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		localInsert(w, r)
	})
	mux.HandleFunc("/query/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		localUpdate(w, r)
	})
	mux.HandleFunc("/query/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		localDelete(w, r)
	})

	log.Printf("Go slave (%s) listening on :8082", selfRole)
	log.Fatal(http.ListenAndServe(":8082", mux))
}

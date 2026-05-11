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
	selfAddr      = "http://127.0.0.1:8082"
	masterAddr    = "http://127.0.0.1:8080"
	listenPort    = ":8082"
)

// Order: master → Python → C# → Go (lowest priority)
var priorityChain = []string{
	"http://127.0.0.1:8080", // master
	"http://127.0.0.1:8083", // Python slave
	"http://127.0.0.1:8081", // C# slave
}

// Peers to broadcast to (everyone except self)
var peers = []string{
	"http://127.0.0.1:8080",
	"http://127.0.0.1:8081",
	"http://127.0.0.1:8083",
}

// Internal secret — must match master and all slaves
const internalSecret = "ddb-internal-secret-2025"

// ── Role state ────────────────────────────────────────────────────────────

var (
	stateMu        sync.RWMutex
	isActingMaster bool
	masterIsDown   bool
)

func setRole(acting bool) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if acting && !isActingMaster {
		log.Println("[role] Promoted to ACTING MASTER")
	} else if !acting && isActingMaster {
		log.Println("[role] Demoted back to SLAVE — pushing snapshot first")
	}
	isActingMaster = acting
}

func canManageDB() bool {
	stateMu.RLock()
	defer stateMu.RUnlock()
	return isActingMaster
}

func selfRole() string {
	stateMu.RLock()
	defer stateMu.RUnlock()
	if isActingMaster {
		return "slave-go (acting master)"
	}
	return "slave-go"
}

// ── Master watcher ────────────────────────────────────────────────────────

func pingNode(url string) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func startMasterWatcher() {
	go func() {
		for {
			time.Sleep(5 * time.Second)

			allHigherDown := true
			for _, node := range priorityChain {
				if pingNode(node) {
					allHigherDown = false
					break
				}
			}

			stateMu.RLock()
			wasActing := isActingMaster
			stateMu.RUnlock()

			if allHigherDown {
				masterIsDown = true
				setRole(true)
			} else {
				masterIsDown = false
				if wasActing {
					go pushSnapshot()
				}
				setRole(false)
			}
		}
	}()
}

func pushSnapshot() {
	snap := buildSnapshot()
	if snap == nil {
		return
	}
	body, _ := json.Marshal(snap)
	req, _ := http.NewRequest("POST", masterAddr+"/replicate/snapshot", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Replication-Secret", internalSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[snapshot] push failed: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[snapshot] pushed to master → HTTP %d", resp.StatusCode)
}

func buildSnapshot() map[string]any {
	rows, err := db.Query(`SELECT schema_name FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema','mysql','performance_schema','sys')`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	databases := map[string]any{}
	for rows.Next() {
		var dbName string
		rows.Scan(&dbName)

		tblRows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = ?", dbName)
		if err != nil {
			continue
		}
		tableMap := map[string]any{}
		for tblRows.Next() {
			var tblName string
			tblRows.Scan(&tblName)

			colRows, _ := db.Query(`SELECT column_name FROM information_schema.columns
				WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`, dbName, tblName)
			var attrs []string
			for colRows.Next() {
				var col string
				colRows.Scan(&col)
				attrs = append(attrs, col)
			}
			colRows.Close()

			dataRows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`.`%s`", dbName, tblName))
			var records []map[string]any
			if err == nil {
				records, _ = scanRows(dataRows)
				dataRows.Close()
			}
			if records == nil {
				records = []map[string]any{}
			}
			tableMap[tblName] = map[string]any{"attributes": attrs, "records": records}
		}
		tblRows.Close()
		databases[dbName] = tableMap
	}
	return map[string]any{"databases": databases}
}

// ── Broadcaster ───────────────────────────────────────────────────────────

func broadcast(path string, payload any) {
	body, _ := json.Marshal(payload)
	for _, peer := range peers {
		go func(url string) {
			if url == masterAddr && masterIsDown {
				return
			}
			req, _ := http.NewRequest("POST", url+path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Replication-Secret", internalSecret)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("[broadcast] %s%s failed: %v", url, path, err)
				return
			}
			defer resp.Body.Close()
		}(peer)
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
	clauses := make([]string, 0)
	args := make([]any, 0)
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
		ptrs := make([]interface{}, len(cols))
		vals := make([]interface{}, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		row := map[string]any{}
		for i, col := range cols {
			if b, ok := vals[i].([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = vals[i]
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func isInternal(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("X-Replication-Secret") == internalSecret {
		return true
	}
	respond(w, http.StatusForbidden, map[string]string{
		"error": "forbidden: internal endpoint",
	})
	return false
}

// ── Health ────────────────────────────────────────────────────────────────

func healthHandler(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, map[string]string{"status": "ok", "role": selfRole()})
}

// ════════════════════════════════════════════════════════════════════════════
//  REPLICATION RECEIVERS  (master/slaves → this node, internal only)
// ════════════════════════════════════════════════════════════════════════════

func replicateCreateDB(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB string `json:"db"`
	}
	decode(r, &req)
	db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateDropDB(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB string `json:"db"`
	}
	decode(r, &req)
	db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", req.DB))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateCreateTable(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	decode(r, &req)
	db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB))
	colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, a := range req.Attributes {
		if strings.ToLower(a) != "id" {
			colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
		}
	}
	db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		req.DB, req.Table, strings.Join(colDefs, ", ")))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateDropTable(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	decode(r, &req)
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", req.DB, req.Table))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateInsert(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	decode(r, &req)
	cols, phs, vals := []string{}, []string{}, []any{}
	for col, val := range req.Record {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		phs = append(phs, "?")
		vals = append(vals, fmt.Sprintf("%v", val))
	}
	db.Exec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		req.DB, req.Table, strings.Join(cols, ", "), strings.Join(phs, ", ")), vals...)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateUpdate(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	decode(r, &req)
	setClauses, args := []string{}, []any{}
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
	db.Exec(query, args...)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateDelete(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	decode(r, &req)
	query := fmt.Sprintf("DELETE FROM `%s`.`%s`", req.DB, req.Table)
	args := []any{}
	if len(req.Where) > 0 {
		cond, whereArgs := buildWhere(req.Where)
		query += " WHERE " + cond
		args = whereArgs
	}
	db.Exec(query, args...)
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

func replicateSnapshot(w http.ResponseWriter, r *http.Request) {
	if !isInternal(w, r) {
		return
	}
	var snapshot struct {
		Databases map[string]map[string]struct {
			Attributes []string         `json:"attributes"`
			Records    []map[string]any `json:"records"`
		} `json:"databases"`
	}
	decode(r, &snapshot)
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
			db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
				dbName, tblName, strings.Join(colDefs, ", ")))
			for _, rec := range tbl.Records {
				cols, phs, vals := []string{}, []string{}, []any{}
				for col, val := range rec {
					cols = append(cols, fmt.Sprintf("`%s`", col))
					phs = append(phs, "?")
					vals = append(vals, fmt.Sprintf("%v", val))
				}
				db.Exec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
					dbName, tblName, strings.Join(cols, ", "), strings.Join(phs, ", ")), vals...)
			}
		}
	}
	respond(w, http.StatusOK, map[string]string{"status": "snapshot applied"})
}

// ════════════════════════════════════════════════════════════════════════════
//  CLIENT ENDPOINTS
// ════════════════════════════════════════════════════════════════════════════

// GET /health already registered above

// POST /db/create  — master-only
func queryCreateDB(w http.ResponseWriter, r *http.Request) {
	if !canManageDB() {
		respond(w, http.StatusForbidden, map[string]string{"error": "only the master can create databases"})
		return
	}
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	broadcast("/replicate/db/create", map[string]any{"db": req.DB})
	respond(w, http.StatusCreated, map[string]string{"message": "database '" + req.DB + "' created", "served_by": selfRole()})
}

// DELETE /db/drop  — master-only
func queryDropDB(w http.ResponseWriter, r *http.Request) {
	if !canManageDB() {
		respond(w, http.StatusForbidden, map[string]string{"error": "only the master can drop databases"})
		return
	}
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}
	if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", req.DB)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	broadcast("/replicate/db/drop", map[string]any{"db": req.DB})
	respond(w, http.StatusOK, map[string]string{"message": "database '" + req.DB + "' dropped", "served_by": selfRole()})
}

// POST /table/create
func queryCreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'attributes' are required"})
		return
	}
	db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB))
	colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, a := range req.Attributes {
		if strings.ToLower(a) != "id" {
			colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
		}
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		req.DB, req.Table, strings.Join(colDefs, ", "))); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	broadcast("/replicate/table/create", map[string]any{"db": req.DB, "table": req.Table, "attributes": req.Attributes})
	respond(w, http.StatusCreated, map[string]string{"message": "table '" + req.Table + "' created", "served_by": selfRole()})
}

// DELETE /table/drop
func queryDropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' and 'table' are required"})
		return
	}
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", req.DB, req.Table)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	broadcast("/replicate/table/drop", map[string]any{"db": req.DB, "table": req.Table})
	respond(w, http.StatusOK, map[string]string{"message": "table '" + req.Table + "' dropped", "served_by": selfRole()})
}

// GET /query/select?db=x&table=y[&col=val...]
func querySelect(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dbName, table := q.Get("db"), q.Get("table")
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
	records, _ := scanRows(rows)
	if records == nil {
		records = []map[string]any{}
	}
	respond(w, http.StatusOK, map[string]any{"count": len(records), "records": records, "served_by": selfRole()})
}

// POST /query/insert
func queryInsert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'record' are required"})
		return
	}
	cols, phs, vals := []string{}, []string{}, []any{}
	for col, val := range req.Record {
		if strings.ToLower(col) == "id" {
			continue
		}
		cols = append(cols, fmt.Sprintf("`%s`", col))
		phs = append(phs, "?")
		vals = append(vals, fmt.Sprintf("%v", val))
	}
	result, err := db.Exec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		req.DB, req.Table, strings.Join(cols, ", "), strings.Join(phs, ", ")), vals...)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	generatedID, _ := result.LastInsertId()
	broadcastRecord := make(map[string]any)
	for k, v := range req.Record {
		broadcastRecord[k] = v
	}
	broadcastRecord["id"] = generatedID
	broadcast("/replicate/query/insert", map[string]any{"db": req.DB, "table": req.Table, "record": broadcastRecord})
	respond(w, http.StatusCreated, map[string]any{"message": "record inserted", "generated_id": generatedID, "served_by": selfRole()})
}

// PUT /query/update
func queryUpdate(w http.ResponseWriter, r *http.Request) {
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
	setClauses, args := []string{}, []any{}
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
	broadcast("/replicate/query/update", map[string]any{"db": req.DB, "table": req.Table, "where": req.Where, "set": req.Set})
	respond(w, http.StatusOK, map[string]any{"message": "update complete", "records_updated": affected, "served_by": selfRole()})
}

// DELETE /query/delete
func queryDelete(w http.ResponseWriter, r *http.Request) {
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
	broadcast("/replicate/query/delete", map[string]any{"db": req.DB, "table": req.Table, "where": req.Where})
	respond(w, http.StatusOK, map[string]any{"message": "delete complete", "records_deleted": affected, "served_by": selfRole()})
}

// ── Main ──────────────────────────────────────────────────────────────────

func main() {
	if err := connectMySQL(); err != nil {
		log.Fatal("Cannot connect to MySQL:", err)
	}
	log.Println("Go slave connected to MySQL")
	startMasterWatcher()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)

	// Replication receivers (internal only)
	mux.HandleFunc("/replicate/db/create", replicateCreateDB)
	mux.HandleFunc("/replicate/db/drop", replicateDropDB)
	mux.HandleFunc("/replicate/table/create", replicateCreateTable)
	mux.HandleFunc("/replicate/table/drop", replicateDropTable)
	mux.HandleFunc("/replicate/query/insert", replicateInsert)
	mux.HandleFunc("/replicate/query/update", replicateUpdate)
	mux.HandleFunc("/replicate/query/delete", replicateDelete)
	mux.HandleFunc("/replicate/snapshot", replicateSnapshot)

	// Client endpoints
	mux.HandleFunc("/db/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			queryCreateDB(w, r)
		}
	})
	mux.HandleFunc("/db/drop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			queryDropDB(w, r)
		}
	})
	mux.HandleFunc("/table/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			queryCreateTable(w, r)
		}
	})
	mux.HandleFunc("/table/drop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			queryDropTable(w, r)
		}
	})
	mux.HandleFunc("/query/select", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			querySelect(w, r)
		}
	})
	mux.HandleFunc("/query/insert", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			queryInsert(w, r)
		}
	})
	mux.HandleFunc("/query/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			queryUpdate(w, r)
		}
	})
	mux.HandleFunc("/query/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			queryDelete(w, r)
		}
	})

	log.Printf("Go slave listening on %s", listenPort)
	log.Fatal(http.ListenAndServe(listenPort, mux))
}

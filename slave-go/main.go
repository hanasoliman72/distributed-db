package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// ── MySQL config ──────────────────────────────────────────────────────────
// Change these to match your MySQL setup (same server, different DB is fine).
const (
	mysqlUser     = "root"
	mysqlPassword = "rootroot"
	mysqlHost     = "127.0.0.1"
	mysqlPort     = "3306"
)

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
	clauses := make([]string, 0, len(where))
	args := make([]any, 0, len(where))
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	return strings.Join(clauses, " AND "), args
}

// scanRows converts sql.Rows → []map[string]any.
// Uses *interface{} so MySQL driver picks the right type per column
// (int64 for INT, string for TEXT, nil for NULL).
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
	respond(w, http.StatusOK, map[string]string{"status": "ok", "role": "slave-go"})
}

// ── Replication receivers ─────────────────────────────────────────────────

// POST /replicate/table/create
// Body: { "db":"mydb", "table":"users", "attributes":["name","age"] }
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

	// Create DB (schema) if it doesn't exist yet on this slave.
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", req.DB)); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Always add AUTO_INCREMENT id first – same as master.
	colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, a := range req.Attributes {
		if strings.ToLower(a) == "id" {
			continue
		}
		colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
	}
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		req.DB, req.Table, strings.Join(colDefs, ", "),
	)
	if _, err := db.Exec(query); err != nil {
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
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", req.DB, req.Table))
	respond(w, http.StatusOK, map[string]string{"status": "replicated"})
}

// POST /replicate/query/insert
// Body: { "db":"mydb", "table":"users", "record":{"id":1,"name":"Ali","age":"20"} }
// The record already contains the master-generated id so both sides stay in sync.
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

	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		req.DB, req.Table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	if _, err := db.Exec(query, values...); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
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

	setClauses := make([]string, 0, len(req.Set))
	args := []any{}
	for col, val := range req.Set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	query := fmt.Sprintf(
		"UPDATE `%s`.`%s` SET %s",
		req.DB, req.Table, strings.Join(setClauses, ", "),
	)
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

// POST /replicate/snapshot
// Full re-sync: wipe all slave data and rebuild from the master snapshot.
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
				if strings.ToLower(a) == "id" {
					continue
				}
				colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
			}
			db.Exec(fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
				dbName, tblName, strings.Join(colDefs, ", "),
			))

			for _, rec := range tbl.Records {
				cols := make([]string, 0)
				placeholders := make([]string, 0)
				values := make([]any, 0)
				for col, val := range rec {
					cols = append(cols, fmt.Sprintf("`%s`", col))
					placeholders = append(placeholders, "?")
					values = append(values, fmt.Sprintf("%v", val))
				}
				db.Exec(fmt.Sprintf(
					"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
					dbName, tblName,
					strings.Join(cols, ", "),
					strings.Join(placeholders, ", "),
				), values...)
			}
		}
	}
	respond(w, http.StatusOK, map[string]string{"status": "snapshot applied"})
}

// ── Local SELECT (read-only, served independently by slave) ───────────────
//
// GET /query/select?db=mydb&table=users              → all records
// GET /query/select?db=mydb&table=users&id=1         → by id
// GET /query/select?db=mydb&table=users&name=Ali     → by name
// GET /query/select?db=mydb&table=users&name=Ali&age=20 → AND filter
func localSelect(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	dbName := q.Get("db")
	table := q.Get("table")
	if dbName == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "query params 'db' and 'table' are required",
		})
		return
	}

	// Every param that is NOT "db" or "table" becomes a WHERE condition.
	where := map[string]any{}
	for key, vals := range q {
		if key == "db" || key == "table" {
			continue
		}
		where[key] = vals[0]
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
		"served_by": "slave-go :8081",
	})
}

// ── Main ──────────────────────────────────────────────────────────────────

func main() {
	if err := connectMySQL(); err != nil {
		log.Fatal("Cannot connect to MySQL:", err)
	}
	log.Println("Go slave connected to MySQL")

	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/health", healthHandler)

	// Replication receivers (called by master, all POST)
	mux.HandleFunc("/replicate/table/create", replicateCreateTable)
	mux.HandleFunc("/replicate/table/drop", replicateDropTable)
	mux.HandleFunc("/replicate/query/insert", replicateInsert)
	mux.HandleFunc("/replicate/query/update", replicateUpdate)
	mux.HandleFunc("/replicate/query/delete", replicateDelete)
	mux.HandleFunc("/replicate/snapshot", replicateSnapshot)

	// Local read query (GET, no body – filters via query params)
	mux.HandleFunc("/query/select", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		localSelect(w, r)
	})

	log.Println("Go slave listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", mux))
}

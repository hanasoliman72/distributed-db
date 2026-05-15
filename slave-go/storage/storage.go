package storage

// storage.go
//
// Each slave has TWO MySQL databases for the same logical shard:
//
//   Primary:  <db>          e.g. "mydb"
//   Replica:  <db>_replica  e.g. "mydb_replica"
//
// On every write the slave writes to BOTH.  On every read the slave tries the
// primary first; if that fails it falls back to the replica.
// This provides fault tolerance against a single-database failure on one PC
// without needing a second physical machine.

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

// replicaName returns the replica schema name for a given shard schema name.
func replicaName(db string) string { return db + "_replica" }

func Connect(dsn string) error {
	var err error
	DB, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	return DB.Ping()
}

// ── Schema helpers ─────────────────────────────────────────────────────────

func CreateDB(db string) error {
	for _, name := range []string{db, replicaName(db)} {
		if _, err := DB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", name)); err != nil {
			return err
		}
	}
	return nil
}

func DropDB(db string) error {
	if !isValidIdentifier(db) {
		return fmt.Errorf("invalid db name: %s", db)
	}
	for _, name := range []string{db, replicaName(db)} {
		if _, err := DB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name)); err != nil {
			return err
		}
	}
	return nil
}

// CreateTable creates the table in both primary and replica schemas.
func CreateTable(db, table string, attributes []string) error {
	if !isValidIdentifier(db) || !isValidIdentifier(table) {
		return fmt.Errorf("invalid db or table name")
	}
	colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, a := range attributes {
		if !isValidIdentifier(a) {
			return fmt.Errorf("invalid attribute name: %s", a)
		}
		if strings.ToLower(a) != "id" {
			colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
		}
	}
	cols := strings.Join(colDefs, ", ")
	for _, schema := range []string{db, replicaName(db)} {
		q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)", schema, table, cols)
		if _, err := DB.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func DropTable(db, table string) error {
	if !isValidIdentifier(db) || !isValidIdentifier(table) {
		return fmt.Errorf("invalid db or table name")
	}
	for _, schema := range []string{db, replicaName(db)} {
		if _, err := DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", schema, table)); err != nil {
			return err
		}
	}
	return nil
}

// ── Row operations ─────────────────────────────────────────────────────────

// InsertRecord inserts into primary and mirrors to replica.
// Returns generated id.
func InsertRecord(db, table string, record map[string]any) (int64, error) {
	cols, phs, vals := buildInsertParts(record, true)
	q := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)", db, table, cols, phs)
	res, err := DB.Exec(q, vals...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()

	// Mirror to replica with explicit id.
	record["id"] = id
	colsR, phsR, valsR := buildInsertParts(record, false)
	qR := fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES (%s)", replicaName(db), table, colsR, phsR)
	if _, err := DB.Exec(qR, valsR...); err != nil {
		log.Printf("[replica] warning: failed to mirror insert to %s.%s: %v", replicaName(db), table, err)
	}

	return id, nil
}

// InsertRecordWithID inserts a row that already has an explicit id (used when
// the gateway re-routes a broadcast with a known id).
func InsertRecordWithID(db, table string, record map[string]any) error {
	cols, phs, vals := buildInsertParts(record, false)
	for _, schema := range []string{db, replicaName(db)} {
		q := fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES (%s)", schema, table, cols, phs)
		if _, err := DB.Exec(q, vals...); err != nil {
			return err
		}
	}
	return nil
}

// SelectRecords reads from the primary; falls back to replica on error.
func SelectRecords(db, table string, where map[string]any) ([]map[string]any, error) {
	rows, err := selectFrom(db, table, where)
	if err != nil {
		// Fallback to replica.
		rows, err = selectFrom(replicaName(db), table, where)
	}
	return rows, err
}

func selectFrom(schema, table string, where map[string]any) ([]map[string]any, error) {
	q := fmt.Sprintf("SELECT * FROM `%s`.`%s`", schema, table)
	args := []any{}
	if len(where) > 0 {
		cond, w := buildWhere(where)
		q += " WHERE " + cond
		args = w
	}
	rows, err := DB.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// UpdateRecords updates primary and replica.
func UpdateRecords(db, table string, where, set map[string]any) (int, error) {
	n, err := updateIn(db, table, where, set)
	if _, rerr := updateIn(replicaName(db), table, where, set); rerr != nil {
		log.Printf("[replica] warning: failed to update replica %s.%s: %v", replicaName(db), table, rerr)
	}
	return n, err
}

func updateIn(schema, table string, where, set map[string]any) (int, error) {
	if len(set) == 0 {
		return 0, fmt.Errorf("set cannot be empty")
	}
	setClauses, args := []string{}, []any{}
	for col, val := range set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, val) // Pass actual type, not stringified
	}
	q := fmt.Sprintf("UPDATE `%s`.`%s` SET %s", schema, table, strings.Join(setClauses, ", "))
	if len(where) > 0 {
		cond, wa := buildWhere(where)
		q += " WHERE " + cond
		args = append(args, wa...)
	}
	res, err := DB.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// DeleteRecords deletes from primary and replica.
func DeleteRecords(db, table string, where map[string]any) (int, error) {
	n, err := deleteFrom(db, table, where)
	if _, derr := deleteFrom(replicaName(db), table, where); derr != nil {
		log.Printf("[replica] warning: failed to delete from replica %s.%s: %v", replicaName(db), table, derr)
	}
	return n, err
}

func deleteFrom(schema, table string, where map[string]any) (int, error) {
	q := fmt.Sprintf("DELETE FROM `%s`.`%s`", schema, table)
	args := []any{}
	if len(where) > 0 {
		cond, wa := buildWhere(where)
		q += " WHERE " + cond
		args = wa
	}
	res, err := DB.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ── Internal helpers ───────────────────────────────────────────────────────

func buildInsertParts(record map[string]any, skipID bool) (cols, placeholders string, vals []any) {
	colList, phList := []string{}, []string{}
	for col, val := range record {
		if skipID && strings.ToLower(col) == "id" {
			continue
		}
		colList = append(colList, fmt.Sprintf("`%s`", col))
		phList = append(phList, "?")
		vals = append(vals, val) // Pass actual type, not stringified
	}
	return strings.Join(colList, ", "), strings.Join(phList, ", "), vals
}

func buildWhere(where map[string]any) (string, []any) {
	clauses, args := []string{}, []any{}
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, val) // Pass actual type, not stringified
	}
	return strings.Join(clauses, " AND "), args
}

// isValidIdentifier checks if a string is a valid SQL identifier
func isValidIdentifier(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	// Allow alphanumeric, underscore, dash (simple validation)
	for _, ch := range s {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
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
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
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

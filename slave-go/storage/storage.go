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
//
// DB-DROP RESILIENCE
// ──────────────────
// When the gateway marks a database as dropped it broadcasts the updated
// metadata snapshot (which includes DroppedDBs) to every slave via
// /shard/metadata/sync. The handler calls MarkDBDropped(db) here, which
// adds the name to a local set. From that point on:
//   - Reads skip the primary attempt entirely (no timeout wait).
//   - Writes go directly to the replica schema.
// This provides instant, zero-latency failover for the storage layer.

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

// ── Dropped-DB local registry ──────────────────────────────────────────────
// Updated by the /shard/metadata/sync handler whenever the gateway tells us
// a database has been physically dropped.

var (
	droppedMu   sync.RWMutex
	localDropped = map[string]struct{}{}
)

// MarkDBDropped registers db as physically dropped on this slave.
// After this call all read/write operations bypass the primary schema.
func MarkDBDropped(db string) {
	droppedMu.Lock()
	localDropped[db] = struct{}{}
	droppedMu.Unlock()
	log.Printf("[storage] DB '%s' marked as dropped — replica-only mode active", db)
}

// ClearDroppedDB removes the dropped mark (used when a DB is re-created).
func ClearDroppedDB(db string) {
	droppedMu.Lock()
	delete(localDropped, db)
	droppedMu.Unlock()
	log.Printf("[storage] DB '%s' cleared from dropped set — primary routing restored", db)
}

// ApplyDroppedDBs atomically REPLACES the entire dropped-DB set with the
// authoritative list from a metadata snapshot. This is the only correct way
// to sync state: a loop that only adds entries would leave stale marks after
// a DB is re-created and removed from the gateway's DroppedDBs list.
func ApplyDroppedDBs(dbs []string) {
	newSet := make(map[string]struct{}, len(dbs))
	for _, d := range dbs {
		newSet[d] = struct{}{}
	}
	droppedMu.Lock()
	localDropped = newSet
	droppedMu.Unlock()
	log.Printf("[storage] dropped-DB set replaced: %v", dbs)
}

// isDropped returns true if the primary database is known to be gone.
func isDropped(db string) bool {
	droppedMu.RLock()
	defer droppedMu.RUnlock()
	_, ok := localDropped[db]
	return ok
}

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
	// If this DB was previously dropped and is now being re-created, clear the mark.
	ClearDroppedDB(db)
	return nil
}

func DropDB(db string) error {
	if !isValidIdentifier(db) {
		return fmt.Errorf("invalid db name: %s", db)
	}
	// *** KEY: only drop the PRIMARY schema. ***
	// The replica (<db>_replica) is intentionally kept alive so that all
	// subsequent reads and writes can transparently fall back to it.
	// Dropping the replica here would leave the system with no data at all.
	if _, err := DB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", db)); err != nil {
		return err
	}
	log.Printf("[storage] primary DB '%s' dropped — replica '%s' retained for fallback", db, replicaName(db))
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

// isUnknownDBError returns true when err is MySQL error 1049 "Unknown database".
// We only call MarkDBDropped for this specific error so that transient failures
// (deadlocks, connection resets, etc.) do NOT permanently redirect all writes
// to the replica — that was the root cause of the insert-inconsistency bug.
func isUnknownDBError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "1049")
}

// InsertRecord inserts into primary and mirrors to replica.
// If the primary DB is known to be dropped, the insert goes directly to the
// replica schema. Only MySQL 1049 "Unknown database" triggers the permanent
// replica-only switch; all other errors are returned to the caller as-is.
func InsertRecord(db, table string, record map[string]any) (int64, error) {
	// Fast-path: primary is known dropped — write only to replica.
	if isDropped(db) {
		return insertIntoReplica(db, table, record)
	}

	// Normal path: try primary first.
	cols, phs, vals := buildInsertParts(record, true)
	q := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)", db, table, cols, phs)
	res, err := DB.Exec(q, vals...)
	if err != nil {
		if isUnknownDBError(err) {
			// Primary DB is gone — permanently switch to replica for this DB.
			log.Printf("[storage] primary DB '%s' gone (1049) — switching to replica-only mode", db)
			MarkDBDropped(db)
			return insertIntoReplica(db, table, record)
		}
		// Transient error — return it; do NOT mark DB as dropped.
		return 0, fmt.Errorf("insert %s.%s: %w", db, table, err)
	}
	id, _ := res.LastInsertId()

	// Mirror to replica with explicit id.
	// Build a copy of record with id set to avoid mutating the caller's map.
	recordWithID := make(map[string]any, len(record)+1)
	for k, v := range record {
		recordWithID[k] = v
	}
	recordWithID["id"] = id
	colsR, phsR, valsR := buildInsertParts(recordWithID, false)
	qR := fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES (%s)", replicaName(db), table, colsR, phsR)
	if _, err := DB.Exec(qR, valsR...); err != nil {
		log.Printf("[replica] warning: failed to mirror insert to %s.%s: %v", replicaName(db), table, err)
	}

	return id, nil
}

// insertIntoReplica inserts directly into the replica schema (primary gone).
// AUTO_INCREMENT is used, so we still get a generated id.
func insertIntoReplica(db, table string, record map[string]any) (int64, error) {
	cols, phs, vals := buildInsertParts(record, true)
	q := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)", replicaName(db), table, cols, phs)
	res, err := DB.Exec(q, vals...)
	if err != nil {
		return 0, fmt.Errorf("replica insert %s.%s: %w", replicaName(db), table, err)
	}
	id, _ := res.LastInsertId()
	log.Printf("[storage] inserted into replica %s.%s (primary dropped), id=%d", db, table, id)
	return id, nil
}

// InsertRecordWithID inserts a row that already has an explicit id (used when
// the gateway re-routes a broadcast with a known id).
func InsertRecordWithID(db, table string, record map[string]any) error {
	schemas := []string{db, replicaName(db)}
	if isDropped(db) {
		// Only replica is available.
		schemas = []string{replicaName(db)}
	}
	cols, phs, vals := buildInsertParts(record, false)
	for _, schema := range schemas {
		q := fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES (%s)", schema, table, cols, phs)
		if _, err := DB.Exec(q, vals...); err != nil {
			if schema == db {
				// Primary failed — mark dropped and try replica only next time.
				log.Printf("[storage] primary InsertWithID failed for %s.%s: %v", db, table, err)
				MarkDBDropped(db)
				continue
			}
			return err
		}
	}
	return nil
}

// SelectRecords reads from the primary; falls back to replica on error or if
// the primary is known to be dropped.
func SelectRecords(db, table string, where map[string]any) ([]map[string]any, error) {
	// Skip primary entirely if we already know it's gone.
	if isDropped(db) {
		return selectFrom(replicaName(db), table, where)
	}
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
// Falls back to replica-only if primary is known dropped.
func UpdateRecords(db, table string, where, set map[string]any) (int, error) {
	if isDropped(db) {
		return updateIn(replicaName(db), table, where, set)
	}
	n, err := updateIn(db, table, where, set)
	if err != nil {
		if isUnknownDBError(err) {
			log.Printf("[storage] primary DB '%s' gone (1049) — switching to replica-only mode", db)
			MarkDBDropped(db)
			return updateIn(replicaName(db), table, where, set)
		}
		return 0, fmt.Errorf("update %s.%s: %w", db, table, err)
	}
	// Mirror to replica.
	if _, rerr := updateIn(replicaName(db), table, where, set); rerr != nil {
		log.Printf("[replica] warning: failed to update replica %s.%s: %v", replicaName(db), table, rerr)
	}
	return n, nil
}

func updateIn(schema, table string, where, set map[string]any) (int, error) {
	if len(set) == 0 {
		return 0, fmt.Errorf("set cannot be empty")
	}
	setClauses, args := []string{}, []any{}
	for col, val := range set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, val)
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
// Falls back to replica-only if primary is known dropped.
func DeleteRecords(db, table string, where map[string]any) (int, error) {
	if isDropped(db) {
		return deleteFrom(replicaName(db), table, where)
	}
	n, err := deleteFrom(db, table, where)
	if err != nil {
		if isUnknownDBError(err) {
			log.Printf("[storage] primary DB '%s' gone (1049) — switching to replica-only mode", db)
			MarkDBDropped(db)
			return deleteFrom(replicaName(db), table, where)
		}
		return 0, fmt.Errorf("delete %s.%s: %w", db, table, err)
	}
	// Mirror delete to replica.
	if _, derr := deleteFrom(replicaName(db), table, where); derr != nil {
		log.Printf("[replica] warning: failed to delete from replica %s.%s: %v", replicaName(db), table, derr)
	}
	return n, nil
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
		vals = append(vals, val)
	}
	return strings.Join(colList, ", "), strings.Join(phList, ", "), vals
}

func buildWhere(where map[string]any) (string, []any) {
	clauses, args := []string{}, []any{}
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, val)
	}
	return strings.Join(clauses, " AND "), args
}

// isValidIdentifier checks if a string is a valid SQL identifier.
func isValidIdentifier(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, ch := range s {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
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

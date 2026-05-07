package storage

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// DB is the shared MySQL connection pool used by all handlers.
var DB *sql.DB

// ── Connection ─────────────────────────────────────────────────────────────

func Connect(dsn string) error {
	var err error
	DB, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	if err = DB.Ping(); err != nil {
		return fmt.Errorf("DB.Ping: %w", err)
	}
	return nil
}

// ── DB operations ──────────────────────────────────────────────────────────

func CreateDB(db string) error {
	_, err := DB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db))
	return err
}

func DropDB(db string) error {
	_, err := DB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", db))
	return err
}

func ListDBs() ([]string, error) {
	rows, err := DB.Query(`
		SELECT schema_name FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema','mysql','performance_schema','sys')
		ORDER BY schema_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		dbs = append(dbs, name)
	}
	return dbs, rows.Err()
}

// ── Table operations ───────────────────────────────────────────────────────

// CreateTable always adds `id INT AUTO_INCREMENT PRIMARY KEY` as the first
// column. The client never sends id on insert – MySQL generates it.
// attributes = user-defined columns e.g. ["name","age","email"].
// If the client accidentally includes "id" in attributes it is skipped.
func CreateTable(db, table string, attributes []string) error {
	if len(attributes) == 0 {
		return fmt.Errorf("at least one attribute is required")
	}

	colDefs := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, a := range attributes {
		if strings.ToLower(a) == "id" {
			continue
		}
		colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", a))
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		db, table, strings.Join(colDefs, ", "),
	)
	_, err := DB.Exec(query)
	return err
}

func DropTable(db, table string) error {
	_, err := DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", db, table))
	return err
}

func ListTables(db string) ([]string, error) {
	rows, err := DB.Query(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ?", db)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func GetTableAttributes(db, table string) ([]string, error) {
	rows, err := DB.Query(
		`SELECT column_name FROM information_schema.columns
		 WHERE table_schema = ? AND table_name = ?
		 ORDER BY ordinal_position`, db, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// ── Record operations ──────────────────────────────────────────────────────

// InsertRecord inserts a record WITHOUT an id – MySQL auto-generates it.
// Returns the generated id so the handler can include it in the response.
// If the client accidentally sends "id" it is silently stripped.
func InsertRecord(db, table string, record map[string]any) (int64, error) {
	if len(record) == 0 {
		return 0, fmt.Errorf("record cannot be empty")
	}

	cols := make([]string, 0, len(record))
	placeholders := make([]string, 0, len(record))
	values := make([]any, 0, len(record))

	for col, val := range record {
		if strings.ToLower(col) == "id" {
			continue // always auto-generated
		}
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, fmt.Sprintf("%v", val))
	}

	if len(cols) == 0 {
		return 0, fmt.Errorf("record has no valid columns after stripping 'id'")
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		db, table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	result, err := DB.Exec(query, values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// SelectRecords returns rows matching ALL key=value pairs in where.
// Pass nil or empty map to return ALL rows.
//
// Supports filtering by any column:
//
//	where = {"id":"3"}              → row with id = 3
//	where = {"name":"Ali"}          → rows where name = Ali
//	where = {"age":"20"}            → rows where age = 20
//	where = {"name":"Ali","age":"20"} → both conditions (AND)
//	where = nil or {}               → all rows
func SelectRecords(db, table string, where map[string]any) ([]map[string]any, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", db, table)
	args := []any{}

	if len(where) > 0 {
		conditions, vals := buildWhere(where)
		query += " WHERE " + conditions
		args = vals
	}

	rows, err := DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func UpdateRecords(db, table string, where, set map[string]any) (int, error) {
	if len(set) == 0 {
		return 0, fmt.Errorf("'set' cannot be empty")
	}

	setClauses := make([]string, 0, len(set))
	args := []any{}
	for col, val := range set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}

	query := fmt.Sprintf(
		"UPDATE `%s`.`%s` SET %s",
		db, table, strings.Join(setClauses, ", "),
	)

	if len(where) > 0 {
		conditions, whereArgs := buildWhere(where)
		query += " WHERE " + conditions
		args = append(args, whereArgs...)
	}

	result, err := DB.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

func DeleteRecords(db, table string, where map[string]any) (int, error) {
	query := fmt.Sprintf("DELETE FROM `%s`.`%s`", db, table)
	args := []any{}

	if len(where) > 0 {
		conditions, whereArgs := buildWhere(where)
		query += " WHERE " + conditions
		args = whereArgs
	}

	result, err := DB.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// ── Snapshot helpers ───────────────────────────────────────────────────────

type TableSnapshot struct {
	Attributes []string         `json:"attributes"`
	Records    []map[string]any `json:"records"`
}

func GetFullTable(db, table string) (*TableSnapshot, error) {
	attrs, err := GetTableAttributes(db, table)
	if err != nil {
		return nil, err
	}
	records, err := SelectRecords(db, table, nil)
	if err != nil {
		return nil, err
	}
	if records == nil {
		records = []map[string]any{}
	}
	return &TableSnapshot{Attributes: attrs, Records: records}, nil
}

func ReplaceTable(db, table string, records []map[string]any, attributes []string) error {
	if err := DropTable(db, table); err != nil {
		return err
	}
	if err := CreateTable(db, table, attributes); err != nil {
		return err
	}
	for _, rec := range records {
		if _, err := InsertRecord(db, table, rec); err != nil {
			return err
		}
	}
	return nil
}

// ── Internal helpers ───────────────────────────────────────────────────────

func buildWhere(where map[string]any) (string, []any) {
	clauses := make([]string, 0, len(where))
	args := make([]any, 0, len(where))
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	return strings.Join(clauses, " AND "), args
}

// scanRows converts sql.Rows into []map[string]any.
//
// FIX: previously used [][]byte which caused id (INT column) to scan as nil.
// Now uses *interface{} per column so MySQL driver picks the correct Go type
// (int64 for INT, string for TEXT, nil for NULL) automatically.
func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		// Use *interface{} so the MySQL driver chooses the right type per column.
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
			raw := values[i]
			// MySQL driver returns TEXT columns as []byte – convert to string.
			// INT columns come as int64 – keep as-is so JSON shows a number.
			// NULL comes as nil – keep as nil so JSON shows null.
			if b, ok := raw.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = raw
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

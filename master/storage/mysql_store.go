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

// Connect opens a connection to MySQL and verifies it is alive.
// Call this once from main.go before starting the HTTP server.
//
// DSN format: "user:password@tcp(host:port)/"
// We do NOT include a database name here because we manage multiple
// databases dynamically.
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

// CreateDB creates a new MySQL database (schema).
func CreateDB(db string) error {
	_, err := DB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db))
	return err
}

// DropDB drops a MySQL database and everything inside it.
func DropDB(db string) error {
	_, err := DB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", db))
	return err
}

// ListDBs returns all user-created databases (excludes MySQL internals).
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

// CreateTable creates a new table inside the given database.
// attributes is a slice like ["id", "name", "age"].
// All columns are created as TEXT for simplicity; the master stores
// whatever JSON the client sends.
func CreateTable(db, table string, attributes []string) error {
	if len(attributes) == 0 {
		return fmt.Errorf("at least one attribute is required")
	}

	// Build column definitions: each attribute becomes a TEXT column.
	cols := make([]string, len(attributes))
	for i, a := range attributes {
		cols[i] = fmt.Sprintf("`%s` TEXT", a)
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		db, table, strings.Join(cols, ", "),
	)
	_, err := DB.Exec(query)
	return err
}

// DropTable drops a table from the given database.
func DropTable(db, table string) error {
	_, err := DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", db, table))
	return err
}

// ListTables returns all table names inside a database.
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

// GetTableAttributes returns the column names of a table in order.
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

// InsertRecord inserts a single record (map of column→value) into the table.
func InsertRecord(db, table string, record map[string]any) error {
	if len(record) == 0 {
		return fmt.Errorf("record cannot be empty")
	}

	// Build:  INSERT INTO `db`.`table` (`col1`,`col2`) VALUES (?,?)
	cols := make([]string, 0, len(record))
	placeholders := make([]string, 0, len(record))
	values := make([]any, 0, len(record))

	for col, val := range record {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, fmt.Sprintf("%v", val))
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		db, table,
		strings.Join(cols, ","),
		strings.Join(placeholders, ","),
	)
	_, err := DB.Exec(query, values...)
	return err
}

// SelectRecords returns rows matching all key=value pairs in where.
// Pass nil or empty map to select all rows.
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

// UpdateRecords updates columns in set for all rows matching where.
// Returns the number of rows affected.
func UpdateRecords(db, table string, where, set map[string]any) (int, error) {
	if len(set) == 0 {
		return 0, fmt.Errorf("'set' cannot be empty")
	}

	// Build SET clause
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

// DeleteRecords removes all rows matching where.
// Returns the number of rows deleted.
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

// ── Snapshot helpers (used by replication) ────────────────────────────────

// TableSnapshot holds all data from one table – sent to slaves on recovery.
type TableSnapshot struct {
	Attributes []string         `json:"attributes"`
	Records    []map[string]any `json:"records"`
}

// GetFullTable returns the schema + all records for one table.
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

// ReplaceTable drops + recreates a table with given data – used by snapshot sync on slaves.
func ReplaceTable(db, table string, records []map[string]any, attributes []string) error {
	if err := DropTable(db, table); err != nil {
		return err
	}
	if err := CreateTable(db, table, attributes); err != nil {
		return err
	}
	for _, rec := range records {
		if err := InsertRecord(db, table, rec); err != nil {
			return err
		}
	}
	return nil
}

// ── Internal helpers ───────────────────────────────────────────────────────

// buildWhere converts a map into a SQL WHERE clause + args slice.
// Example: {"name":"Ali","age":"20"} → "`name` = ? AND `age` = ?", ["Ali","20"]
func buildWhere(where map[string]any) (string, []any) {
	clauses := make([]string, 0, len(where))
	args := make([]any, 0, len(where))
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("`%s` = ?", col))
		args = append(args, fmt.Sprintf("%v", val))
	}
	return strings.Join(clauses, " AND "), args
}

// scanRows converts sql.Rows into a slice of maps (column→value).
func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		// Create a slice of interface{} to hold each column value.
		values := make([][]byte, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = string(values[i])
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

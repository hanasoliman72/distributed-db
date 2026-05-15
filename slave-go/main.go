package main

// slave-go/main.go
//
// Go slave node.  Handles only its assigned shard of the data.
//
// Security:
//   Every request must carry a valid X-Gateway-Token header signed by the
//   shared HMAC secret.  Requests without a valid token are rejected 403.
//
// Fault tolerance:
//   Reads fall back to the local replica schema if the primary fails.
//   Writes go to both primary and replica.
//
// Endpoints (all require X-Gateway-Token):
//   POST   /shard/db/create
//   DELETE /shard/db/drop
//   POST   /shard/table/create
//   DELETE /shard/table/drop
//   POST   /shard/query/insert
//   GET    /shard/query/select
//   PUT    /shard/query/update
//   DELETE /shard/query/delete
//   GET    /shard/query/search
//   GET    /health   ← no token required (used by gateway health checker)

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"slave/storage"
	"strings"
)

// ── Auth ──────────────────────────────────────────────────────────────────

const (
	defaultSharedSecret = "Hana-1234"
	defaultMysqlHost    = "127.0.0.1"
	defaultMysqlPort    = "3306"
	defaultMysqlUser    = "root"
	defaultMysqlPass    = "root"
	defaultSlavePort    = ":8081"
)

var (
	sharedSecretStr string
	mysqlHost       string
	mysqlPort       string
	mysqlUser       string
	mysqlPassword   string
	slavePort       string
)

var sharedSecret []byte

func initEnv() {
	sharedSecretStr = getEnv("SLAVE_SHARED_SECRET", defaultSharedSecret)
	mysqlHost = getEnv("MYSQL_HOST", defaultMysqlHost)
	mysqlPort = getEnv("MYSQL_PORT", defaultMysqlPort)
	mysqlUser = getEnv("MYSQL_USER", defaultMysqlUser)
	mysqlPassword = getEnv("MYSQL_PASSWORD", defaultMysqlPass)
	slavePort = getEnv("SLAVE_PORT", defaultSlavePort)

	sharedSecret = []byte(sharedSecretStr)
	log.Println("[slave-go] configuration loaded from environment")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func verifyToken(token string) error {
	parts := strings.SplitN(token, "|", 3)
	if len(parts) != 3 {
		return fmt.Errorf("malformed token")
	}
	ts, nonce, gotSig := parts[0], parts[1], parts[2]

	mac := hmac.New(sha256.New, sharedSecret)
	mac.Write([]byte(ts + "|" + nonce))
	wantSig := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(gotSig), []byte(wantSig)) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

// authMiddleware wraps a handler and rejects requests with missing/invalid tokens.
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-Gateway-Token")
		if token == "" {
			respond(w, http.StatusForbidden, map[string]string{"error": "missing X-Gateway-Token"})
			return
		}
		if err := verifyToken(token); err != nil {
			respond(w, http.StatusForbidden, map[string]string{"error": "invalid token: " + err.Error()})
			return
		}
		next(w, r)
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

// ── Handlers ──────────────────────────────────────────────────────────────

func createDBHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	decode(r, &req)
	if err := storage.CreateDB(req.DB); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusCreated, map[string]string{"status": "ok"})
}

func dropDBHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db required"})
		return
	}
	if err := storage.DropDB(req.DB); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "ok"})
}

func createTableHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	decode(r, &req)
	if err := storage.CreateTable(req.DB, req.Table, req.Attributes); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusCreated, map[string]string{"status": "ok"})
}

func dropTableHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db and table required"})
		return
	}
	if err := storage.DropTable(req.DB, req.Table); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "ok"})
}

func insertHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB       string         `json:"db"`
		Table    string         `json:"table"`
		Record   map[string]any `json:"record"`
		ShardIdx int            `json:"shard_idx"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db, table, record required"})
		return
	}

	id, err := storage.InsertRecord(req.DB, req.Table, req.Record)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusCreated, map[string]any{"message": "record inserted", "generated_id": id})
}

func selectHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db := q.Get("db")
	table := q.Get("table")
	if db == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db and table required"})
		return
	}
	where := map[string]any{}
	for k, vals := range q {
		if k != "db" && k != "table" {
			where[k] = vals[0]
		}
	}
	records, err := storage.SelectRecords(db, table, where)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if records == nil {
		records = []map[string]any{}
	}
	respond(w, http.StatusOK, map[string]any{"count": len(records), "records": records})
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	if err := decode(r, &req); err != nil || req.Set == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db, table, set required"})
		return
	}
	n, err := storage.UpdateRecords(req.DB, req.Table, req.Where, req.Set)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]any{"message": "update complete", "records_updated": n})
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db and table required"})
		return
	}
	n, err := storage.DeleteRecords(req.DB, req.Table, req.Where)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]any{"message": "delete complete", "records_deleted": n})
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db, table, term := q.Get("db"), q.Get("table"), strings.TrimSpace(q.Get("q"))
	if db == "" || table == "" || term == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "db, table, q required"})
		return
	}
	all, err := storage.SelectRecords(db, table, nil)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	termLower := strings.ToLower(term)
	matched := []map[string]any{}
	for _, row := range all {
		for _, v := range row {
			if strings.Contains(strings.ToLower(fmt.Sprintf("%v", v)), termLower) {
				matched = append(matched, row)
				break
			}
		}
	}
	respond(w, http.StatusOK, map[string]any{"search_term": term, "count": len(matched), "records": matched})
}

// ── Main ──────────────────────────────────────────────────────────────────

func main() {
	initEnv()

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/",
		mysqlUser,
		mysqlPassword,
		mysqlHost,
		mysqlPort,
	)
	if err := storage.Connect(dsn); err != nil {
		log.Fatalf("[slave-go] cannot connect to MySQL: %v", err)
	}
	log.Println("[slave-go] connected to MySQL")

	mux := http.NewServeMux()

	// Health – no auth (gateway pings this).
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "ok", "role": "slave-go"})
	})

	// All shard endpoints require a valid gateway token.
	mux.HandleFunc("/shard/db/create", authMiddleware(createDBHandler))
	mux.HandleFunc("/shard/db/drop", authMiddleware(dropDBHandler))
	mux.HandleFunc("/shard/table/create", authMiddleware(createTableHandler))
	mux.HandleFunc("/shard/table/drop", authMiddleware(dropTableHandler))
	mux.HandleFunc("/shard/query/insert", authMiddleware(insertHandler))
	mux.HandleFunc("/shard/query/select", authMiddleware(selectHandler))
	mux.HandleFunc("/shard/query/update", authMiddleware(updateHandler))
	mux.HandleFunc("/shard/query/delete", authMiddleware(deleteHandler))
	mux.HandleFunc("/shard/query/search", authMiddleware(searchHandler))

	log.Printf("[slave-go] listening on %s", slavePort)
	log.Fatal(http.ListenAndServe(slavePort, mux))
}

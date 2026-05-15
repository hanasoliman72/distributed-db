package main

// slave-go/main.go
//
// Go slave node — handles its assigned shard of the data.
//
// FAILOVER ROLE
// If the gateway at :8080 fails to respond to /health for
// gatewayMissedPings consecutive checks, this slave promotes itself:
//   1. Reads metadata.json (written by the gateway on every DDL change).
//   2. Loads the shard map and slave registry into memory.
//   3. Starts the full gateway HTTP mux on :8080 in a new goroutine,
//      including DDL / DML / auth-forwarding handlers.
//   4. Continues serving its own shard on its original port (:8081).
//
// The promoted slave signs outbound requests to the other slaves using the
// same HMAC secret, so they never notice the switch.
//
// Security:
//   Every /shard/* request must carry a valid X-Gateway-Token header.
//   /health is public.
//
// Fault tolerance:
//   Reads fall back to the local _replica schema if the primary fails.
//   Writes go to both primary and replica.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"slave/storage"
)

// ── Constants ─────────────────────────────────────────────────────────────

const (
	defaultSharedSecret = "Hana-1234"
	defaultMysqlHost    = "127.0.0.1"
	defaultMysqlPort    = "3306"
	defaultMysqlUser    = "root"
	defaultMysqlPass    = "root"
	defaultSlavePort    = ":8081"
	defaultSlaveID      = "slave-a"
	defaultMetadataFile = "metadata.json"

	gatewayURL           = "http://127.0.0.1:8080"
	gatewayCheckInterval = 3 * time.Second
	gatewayMissedPings   = 3
)

// ── Runtime state ─────────────────────────────────────────────────────────

var (
	sharedSecretStr string
	mysqlHost       string
	mysqlPort       string
	mysqlUser       string
	mysqlPassword   string
	slavePort       string
	slaveID         string
	metadataFile    string
	sharedSecret    []byte

	// isGateway is set to 1 atomically when this slave promotes itself.
	isGateway int32

	// Demotion control: when the original gateway comes back online,
	// the promoted slave will gracefully shut down and yield :8080.
	promoServer     *http.Server
	promoDemoteChan = make(chan struct{})
	promoCtx        context.Context
	promoCancel     context.CancelFunc
)

func initEnv() {
	sharedSecretStr = getEnv("SLAVE_SHARED_SECRET", defaultSharedSecret)
	mysqlHost = getEnv("MYSQL_HOST", defaultMysqlHost)
	mysqlPort = getEnv("MYSQL_PORT", defaultMysqlPort)
	mysqlUser = getEnv("MYSQL_USER", defaultMysqlUser)
	mysqlPassword = getEnv("MYSQL_PASSWORD", defaultMysqlPass)
	slavePort = getEnv("SLAVE_PORT", defaultSlavePort)
	slaveID = getEnv("SLAVE_ID", defaultSlaveID)
	metadataFile = getEnv("SLAVE_METADATA_FILE", defaultMetadataFile)
	sharedSecret = []byte(sharedSecretStr)
	log.Println("[slave-go] configuration loaded from environment")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── HMAC — verifying inbound tokens (slave role) ──────────────────────────
//
// Token format: <nonce>|<HMAC-SHA256(secret, nonce)>
// No timestamp — the 16-byte random nonce is the uniqueness guarantee.

func verifyToken(token string) error {
	parts := strings.SplitN(token, "|", 2)
	if len(parts) != 2 {
		return fmt.Errorf("malformed token (expected nonce|sig)")
	}
	nonce, gotSig := parts[0], parts[1]
	if len(nonce) < 16 {
		return fmt.Errorf("nonce too short")
	}

	mac := hmac.New(sha256.New, sharedSecret)
	mac.Write([]byte(nonce))
	wantSig := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(gotSig), []byte(wantSig)) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

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

// ── HMAC — minting outbound tokens (gateway role) ─────────────────────────
//
// Token format: <nonce>|<HMAC-SHA256(secret, nonce)>

func newToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(buf)
	mac := hmac.New(sha256.New, sharedSecret)
	mac.Write([]byte(nonce))
	sig := hex.EncodeToString(mac.Sum(nil))
	return nonce + "|" + sig, nil
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

// ── Own-shard handlers ────────────────────────────────────────────────────

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
	db, table := q.Get("db"), q.Get("table")
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

// ── Gateway watchdog ──────────────────────────────────────────────────────

func watchGateway() {
	cl := &http.Client{Timeout: 2 * time.Second}
	missed := 0
	log.Printf("[slave-go] watchdog: pinging %s every %v", gatewayURL, gatewayCheckInterval)

	for {
		time.Sleep(gatewayCheckInterval)
		if atomic.LoadInt32(&isGateway) == 1 {
			return
		}
		resp, err := cl.Get(gatewayURL + "/health")
		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			missed = 0
			continue
		}
		if resp != nil {
			resp.Body.Close()
		}
		missed++
		log.Printf("[slave-go] watchdog: gateway missed %d/%d", missed, gatewayMissedPings)
		if missed >= gatewayMissedPings {
			log.Println("[slave-go] gateway is DOWN — promoting self")
			promote()
			return
		}
	}
}

// ── Promoted-gateway state ────────────────────────────────────────────────

var (
	promoMu     sync.RWMutex
	promoSlaves []*promoSlave
	promoTables = map[string]*promoTableMeta{}
	promoRRMu   sync.Mutex
	promoRRCtr  int
)

type promoSlave struct {
	ID    string
	URL   string
	mu    sync.RWMutex
	alive bool
}

func (s *promoSlave) IsAlive() bool   { s.mu.RLock(); defer s.mu.RUnlock(); return s.alive }
func (s *promoSlave) SetAlive(v bool) { s.mu.Lock(); defer s.mu.Unlock(); s.alive = v }

type promoTableMeta struct {
	DB         string   `json:"DB"`
	Table      string   `json:"Table"`
	Attributes []string `json:"Attributes"`
	ShardCount int      `json:"ShardCount"`
	SlaveIDs   []string `json:"SlaveIDs"`
}

// promote loads metadata.json and starts the gateway mux on :8080.
// Also starts a demotion checker to gracefully shut down if the original gateway recovers.
func promote() {
	atomic.StoreInt32(&isGateway, 1)
	if err := loadPromotedMetadata(); err != nil {
		log.Fatalf("[slave-go] promote: cannot load metadata: %v", err)
	}
	log.Printf("[slave-go] promote: %d slaves, %d tables loaded", len(promoSlaves), len(promoTables))

	// Create context for graceful shutdown
	promoCtx, promoCancel = context.WithCancel(context.Background())

	go promoHealthChecker(10 * time.Second)
	go demotionChecker(10 * time.Second)

	go func() {
		mux := buildGatewayMux()
		promoServer = &http.Server{
			Addr:    ":8080",
			Handler: mux,
		}
		log.Println("[slave-go] *** PROMOTED — now acting as GATEWAY on :8080 ***")
		if err := promoServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[slave-go] promoted gateway error: %v", err)
		}
	}()
}

// demotionChecker monitors the original gateway and triggers demotion if it comes back online.
func demotionChecker(interval time.Duration) {
	cl := &http.Client{Timeout: 2 * time.Second}
	log.Printf("[slave-go] demotion checker: will monitor %s every %v", gatewayURL, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if the original gateway is back online
			resp, err := cl.Get(gatewayURL + "/health")
			if err != nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				log.Println("[slave-go] demotion: original gateway is back ONLINE — demoting self")
				demote()
				return
			}
			resp.Body.Close()
		case <-promoDemoteChan:
			log.Println("[slave-go] demotion: signal received — demoting self")
			demote()
			return
		}
	}
}

// demote gracefully shuts down the promoted gateway and returns to slave-only mode.
func demote() {
	atomic.StoreInt32(&isGateway, 0)
	if promoCancel != nil {
		promoCancel()
	}
	// Try to sync metadata back to the original gateway before shutting down.
	syncMetadataToGateway()
	if promoServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := promoServer.Shutdown(ctx); err != nil {
			log.Printf("[slave-go] demote: error shutting down gateway server: %v", err)
		}
		promoServer = nil
		log.Println("[slave-go] *** DEMOTED — back to SLAVE-ONLY mode on :8081 ***")
	}
	// Restart the watchdog to monitor the gateway again
	go watchGateway()
}

func loadPromotedMetadata() error {
	data, err := readMetadataFile()
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[slave-go] promote: %s not found, bootstrapping self-only gateway metadata", metadataFile)
			promoSlaves = []*promoSlave{{ID: slaveID, URL: "http://127.0.0.1" + slavePort, alive: true}}
			promoTables = map[string]*promoTableMeta{}
			return nil
		}
		return err
	}
	var pm struct {
		Slaves []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"slaves"`
		Tables []*promoTableMeta `json:"tables"`
	}
	if err := json.Unmarshal(data, &pm); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	promoMu.Lock()
	defer promoMu.Unlock()
	promoSlaves = make([]*promoSlave, 0, len(pm.Slaves)+1)
	selfURL := "http://127.0.0.1" + slavePort
	selfAdded := false
	for _, s := range pm.Slaves {
		if s.ID == slaveID {
			promoSlaves = append(promoSlaves, &promoSlave{ID: s.ID, URL: selfURL, alive: true})
			selfAdded = true
			continue
		}
		promoSlaves = append(promoSlaves, &promoSlave{ID: s.ID, URL: s.URL, alive: true})
	}
	if !selfAdded {
		promoSlaves = append([]*promoSlave{{ID: slaveID, URL: selfURL, alive: true}}, promoSlaves...)
	}
	promoTables = make(map[string]*promoTableMeta, len(pm.Tables))
	for _, t := range pm.Tables {
		promoTables[t.DB+"."+t.Table] = t
	}
	return nil
}

func readMetadataFile() ([]byte, error) {
	data, err := os.ReadFile(metadataFile)
	if err == nil {
		return data, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", metadataFile, err)
	}
	fallbacks := []string{"../gateway/metadata.json", "../../gateway/metadata.json"}
	for _, path := range fallbacks {
		data, ferr := os.ReadFile(path)
		if ferr == nil {
			log.Printf("[slave-go] promote: loaded metadata from fallback %s", path)
			return data, nil
		}
		if !os.IsNotExist(ferr) {
			return nil, fmt.Errorf("read %s: %w", path, ferr)
		}
	}
	return nil, err
}

func savePromotedMetadata() {
	promoMu.RLock()
	slaves := make([]map[string]string, len(promoSlaves))
	for i, s := range promoSlaves {
		slaves[i] = map[string]string{"id": s.ID, "url": s.URL}
	}
	tables := make([]*promoTableMeta, 0, len(promoTables))
	for _, t := range promoTables {
		tables = append(tables, t)
	}
	promoMu.RUnlock()
	data, _ := json.MarshalIndent(map[string]any{"slaves": slaves, "tables": tables}, "", "  ")
	os.WriteFile(metadataFile, data, 0644)
}

// syncMetadataToGateway posts the promoted node's metadata.json to the original
// gateway so it can reload any DDL changes that happened while it was down.
func syncMetadataToGateway() {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		log.Printf("[slave-go] sync: cannot read %s: %v", metadataFile, err)
		return
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Post(gatewayURL+"/gateway/sync", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Printf("[slave-go] sync: POST failed: %v", err)
		return
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[slave-go] sync: gateway returned status %d", resp.StatusCode)
		return
	}
	log.Println("[slave-go] sync: metadata synced to original gateway")
}

func promoHealthChecker(interval time.Duration) {
	cl := &http.Client{Timeout: 3 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		promoMu.RLock()
		slaves := make([]*promoSlave, len(promoSlaves))
		copy(slaves, promoSlaves)
		promoMu.RUnlock()
		for _, s := range slaves {
			go func(sl *promoSlave) {
				resp, err := cl.Get(sl.URL + "/health")
				if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
					if resp != nil {
						resp.Body.Close()
					}
					if sl.IsAlive() {
						log.Printf("[slave-go/gw] slave %s OFFLINE", sl.ID)
					}
					sl.SetAlive(false)
				} else {
					if !sl.IsAlive() {
						log.Printf("[slave-go/gw] slave %s ONLINE", sl.ID)
					}
					sl.SetAlive(true)
					resp.Body.Close()
				}
			}(s)
		}
	}
}

func alivePromoSlaves() []*promoSlave {
	promoMu.RLock()
	defer promoMu.RUnlock()
	out := make([]*promoSlave, 0)
	for _, s := range promoSlaves {
		if s.IsAlive() {
			out = append(out, s)
		}
	}
	return out
}

func slaveByID(id string) *promoSlave {
	promoMu.RLock()
	defer promoMu.RUnlock()
	for _, s := range promoSlaves {
		if s.ID == id {
			return s
		}
	}
	return nil
}

func fnvHash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func slaveForID(meta *promoTableMeta, id string) *promoSlave {
	idx := int(fnvHash(id)) % meta.ShardCount
	return slaveByID(meta.SlaveIDs[idx])
}

func nextInsertSlave(meta *promoTableMeta) (*promoSlave, int, error) {
	promoRRMu.Lock()
	defer promoRRMu.Unlock()
	for attempt := 0; attempt < meta.ShardCount; attempt++ {
		idx := (promoRRCtr + attempt) % meta.ShardCount
		s := slaveByID(meta.SlaveIDs[idx])
		if s != nil && s.IsAlive() {
			promoRRCtr = (idx + 1) % meta.ShardCount
			return s, idx, nil
		}
	}
	return nil, 0, fmt.Errorf("no alive slave for %s.%s", meta.DB, meta.Table)
}

// ── HTTP forwarding ───────────────────────────────────────────────────────

var promoHTTPClient = &http.Client{Timeout: 10 * time.Second}

type fwdResult struct {
	SlaveID    string
	StatusCode int
	Body       map[string]any
	Err        error
}

func fwdJSON(sl *promoSlave, method, endpoint string, payload any) fwdResult {
	body, err := json.Marshal(payload)
	if err != nil {
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	token, err := newToken()
	if err != nil {
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	req, err := http.NewRequest(method, sl.URL+endpoint, bytes.NewReader(body))
	if err != nil {
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Gateway-Token", token)
	resp, err := promoHTTPClient.Do(req)
	if err != nil {
		sl.SetAlive(false)
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var rb map[string]any
	json.Unmarshal(raw, &rb)
	if resp.StatusCode >= 500 {
		sl.SetAlive(false)
	}
	return fwdResult{SlaveID: sl.ID, StatusCode: resp.StatusCode, Body: rb}
}

func fwdGET(sl *promoSlave, path string) fwdResult {
	token, err := newToken()
	if err != nil {
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	req, err := http.NewRequest("GET", sl.URL+path, nil)
	if err != nil {
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	req.Header.Set("X-Gateway-Token", token)
	resp, err := promoHTTPClient.Do(req)
	if err != nil {
		sl.SetAlive(false)
		return fwdResult{SlaveID: sl.ID, Err: err}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var rb map[string]any
	json.Unmarshal(raw, &rb)
	return fwdResult{SlaveID: sl.ID, StatusCode: resp.StatusCode, Body: rb}
}

func broadcastAll(method, endpoint string, payload any) []fwdResult {
	alive := alivePromoSlaves()
	if len(alive) == 0 {
		return nil
	}
	ch := make(chan fwdResult, len(alive))
	for _, s := range alive {
		go func(sl *promoSlave) { ch <- fwdJSON(sl, method, endpoint, payload) }(s)
	}
	results := make([]fwdResult, 0, len(alive))
	for range alive {
		results = append(results, <-ch)
	}
	close(ch)
	return results
}

func routeWriteTargets(db, table string, where map[string]any) []*promoSlave {
	if idVal, ok := where["id"]; ok {
		promoMu.RLock()
		meta := promoTables[db+"."+table]
		promoMu.RUnlock()
		if meta != nil {
			if t := slaveForID(meta, fmt.Sprintf("%v", idVal)); t != nil && t.IsAlive() {
				return []*promoSlave{t}
			}
		}
	}
	return alivePromoSlaves()
}

// ── Promoted gateway mux ──────────────────────────────────────────────────

func gwMethod(m string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != m {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

func buildGatewayMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "ok", "role": "gateway", "promoted_by": "slave-a"})
	})

	mux.HandleFunc("/gateway/status", func(w http.ResponseWriter, r *http.Request) {
		promoMu.RLock()
		type si struct {
			ID    string `json:"id"`
			URL   string `json:"url"`
			Alive bool   `json:"alive"`
		}
		out := make([]si, len(promoSlaves))
		for i, s := range promoSlaves {
			out[i] = si{s.ID, s.URL, s.IsAlive()}
		}
		promoMu.RUnlock()
		respond(w, http.StatusOK, map[string]any{"slaves": out, "promoted_gateway": "slave-a"})
	})

	// ── DDL ───────────────────────────────────────────────────────────

	mux.HandleFunc("/db/create", gwMethod("POST", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB string `json:"db"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
			return
		}
		results := broadcastAll("POST", "/shard/db/create", map[string]any{"db": req.DB})
		if len(results) == 0 {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
			return
		}
		for _, res := range results {
			if res.Err != nil || res.StatusCode >= 400 {
				respond(w, http.StatusInternalServerError, map[string]string{"error": "failed on " + res.SlaveID})
				return
			}
		}
		savePromotedMetadata()
		respond(w, http.StatusCreated, map[string]string{"message": "database '" + req.DB + "' created"})
	}))

	mux.HandleFunc("/db/drop", gwMethod("DELETE", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB string `json:"db"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
			return
		}
		promoMu.Lock()
		for k, t := range promoTables {
			if t.DB == req.DB {
				delete(promoTables, k)
			}
		}
		promoMu.Unlock()
		broadcastAll("DELETE", "/shard/db/drop", map[string]any{"db": req.DB})
		savePromotedMetadata()
		respond(w, http.StatusOK, map[string]string{"message": "database '" + req.DB + "' dropped"})
	}))

	mux.HandleFunc("/table/create", gwMethod("POST", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB         string   `json:"db"`
			Table      string   `json:"table"`
			Attributes []string `json:"attributes"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || len(req.Attributes) == 0 {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table','attributes' required"})
			return
		}
		alive := alivePromoSlaves()
		if len(alive) == 0 {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
			return
		}
		ids := make([]string, len(alive))
		for i, s := range alive {
			ids[i] = s.ID
		}
		meta := &promoTableMeta{DB: req.DB, Table: req.Table, Attributes: req.Attributes, ShardCount: len(alive), SlaveIDs: ids}
		promoMu.Lock()
		promoTables[req.DB+"."+req.Table] = meta
		promoMu.Unlock()
		results := broadcastAll("POST", "/shard/table/create", map[string]any{"db": req.DB, "table": req.Table, "attributes": req.Attributes})
		for _, res := range results {
			if res.Err != nil || res.StatusCode >= 400 {
				respond(w, http.StatusInternalServerError, map[string]string{"error": "failed on " + res.SlaveID})
				return
			}
		}
		savePromotedMetadata()
		respond(w, http.StatusCreated, map[string]any{"message": "table '" + req.Table + "' created", "shards": ids})
	}))

	mux.HandleFunc("/table/drop", gwMethod("DELETE", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB    string `json:"db"`
			Table string `json:"table"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table' required"})
			return
		}
		promoMu.Lock()
		delete(promoTables, req.DB+"."+req.Table)
		promoMu.Unlock()
		broadcastAll("DELETE", "/shard/table/drop", map[string]any{"db": req.DB, "table": req.Table})
		savePromotedMetadata()
		respond(w, http.StatusOK, map[string]string{"message": "table '" + req.Table + "' dropped"})
	}))

	// ── DML ───────────────────────────────────────────────────────────

	mux.HandleFunc("/query/insert", gwMethod("POST", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB     string         `json:"db"`
			Table  string         `json:"table"`
			Record map[string]any `json:"record"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table','record' required"})
			return
		}
		promoMu.RLock()
		meta := promoTables[req.DB+"."+req.Table]
		promoMu.RUnlock()
		if meta == nil {
			respond(w, http.StatusNotFound, map[string]string{"error": "table not found; create it first"})
			return
		}
		target, shardIdx, err := nextInsertSlave(meta)
		if err != nil {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		res := fwdJSON(target, "POST", "/shard/query/insert", map[string]any{
			"db": req.DB, "table": req.Table, "record": req.Record, "shard_idx": shardIdx,
		})
		if res.Err != nil {
			respond(w, http.StatusBadGateway, map[string]string{"error": res.Err.Error()})
			return
		}
		res.Body["shard"] = target.ID
		respond(w, res.StatusCode, res.Body)
	}))

	mux.HandleFunc("/query/select", gwMethod("GET", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		db, table := q.Get("db"), q.Get("table")
		if db == "" || table == "" {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table' required"})
			return
		}
		if idVal := q.Get("id"); idVal != "" {
			promoMu.RLock()
			meta := promoTables[db+"."+table]
			promoMu.RUnlock()
			if meta != nil {
				if t := slaveForID(meta, idVal); t != nil && t.IsAlive() {
					res := fwdGET(t, "/shard/query/select?"+q.Encode())
					if res.Err == nil {
						respond(w, res.StatusCode, res.Body)
						return
					}
				}
			}
		}
		alive := alivePromoSlaves()
		if len(alive) == 0 {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
			return
		}
		ch := make(chan []any, len(alive))
		path := "/shard/query/select?" + q.Encode()
		for _, s := range alive {
			go func(sl *promoSlave) {
				res := fwdGET(sl, path)
				if res.Err != nil {
					ch <- nil
					return
				}
				rows, _ := res.Body["records"].([]any)
				ch <- rows
			}(s)
		}
		merged := make([]any, 0)
		for range alive {
			if rows := <-ch; rows != nil {
				merged = append(merged, rows...)
			}
		}
		close(ch)
		respond(w, http.StatusOK, map[string]any{"count": len(merged), "records": merged})
	}))

	mux.HandleFunc("/query/update", gwMethod("PUT", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB    string         `json:"db"`
			Table string         `json:"table"`
			Where map[string]any `json:"where"`
			Set   map[string]any `json:"set"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Set == nil {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table','set' required"})
			return
		}
		targets := routeWriteTargets(req.DB, req.Table, req.Where)
		if len(targets) == 0 {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
			return
		}
		payload := map[string]any{"db": req.DB, "table": req.Table, "where": req.Where, "set": req.Set}
		total := 0
		for _, t := range targets {
			res := fwdJSON(t, "PUT", "/shard/query/update", payload)
			if res.Err == nil {
				if n, ok := res.Body["records_updated"].(float64); ok {
					total += int(n)
				}
			}
		}
		respond(w, http.StatusOK, map[string]any{"message": "update complete", "records_updated": total})
	}))

	mux.HandleFunc("/query/delete", gwMethod("DELETE", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DB    string         `json:"db"`
			Table string         `json:"table"`
			Where map[string]any `json:"where"`
		}
		if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table' required"})
			return
		}
		targets := routeWriteTargets(req.DB, req.Table, req.Where)
		if len(targets) == 0 {
			respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
			return
		}
		payload := map[string]any{"db": req.DB, "table": req.Table, "where": req.Where}
		total := 0
		for _, t := range targets {
			res := fwdJSON(t, "DELETE", "/shard/query/delete", payload)
			if res.Err == nil {
				if n, ok := res.Body["records_deleted"].(float64); ok {
					total += int(n)
				}
			}
		}
		respond(w, http.StatusOK, map[string]any{"message": "delete complete", "records_deleted": total})
	}))

	mux.HandleFunc("/query/search", gwMethod("GET", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		db, table, term := q.Get("db"), q.Get("table"), q.Get("q")
		if db == "" || table == "" || term == "" {
			respond(w, http.StatusBadRequest, map[string]string{"error": "'db','table','q' required"})
			return
		}
		alive := alivePromoSlaves()
		ch := make(chan []any, len(alive))
		path := fmt.Sprintf("/shard/query/search?db=%s&table=%s&q=%s", db, table, url.QueryEscape(term))
		for _, s := range alive {
			go func(sl *promoSlave) {
				res := fwdGET(sl, path)
				if res.Err != nil || res.StatusCode >= 400 {
					ch <- nil
					return
				}
				rows, _ := res.Body["records"].([]any)
				ch <- rows
			}(s)
		}
		merged := make([]any, 0)
		for range alive {
			if rows := <-ch; rows != nil {
				merged = append(merged, rows...)
			}
		}
		close(ch)
		respond(w, http.StatusOK, map[string]any{"search_term": term, "count": len(merged), "records": merged})
	}))

	return mux
}

// ── Main ──────────────────────────────────────────────────────────────────

func main() {
	initEnv()

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", mysqlUser, mysqlPassword, mysqlHost, mysqlPort)
	if err := storage.Connect(dsn); err != nil {
		log.Fatalf("[slave-go] cannot connect to MySQL: %v", err)
	}
	log.Println("[slave-go] connected to MySQL")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "ok", "role": "slave-go"})
	})
	mux.HandleFunc("/shard/db/create", authMiddleware(createDBHandler))
	mux.HandleFunc("/shard/db/drop", authMiddleware(dropDBHandler))
	mux.HandleFunc("/shard/table/create", authMiddleware(createTableHandler))
	mux.HandleFunc("/shard/table/drop", authMiddleware(dropTableHandler))
	mux.HandleFunc("/shard/query/insert", authMiddleware(insertHandler))
	mux.HandleFunc("/shard/query/select", authMiddleware(selectHandler))
	mux.HandleFunc("/shard/query/update", authMiddleware(updateHandler))
	mux.HandleFunc("/shard/query/delete", authMiddleware(deleteHandler))
	mux.HandleFunc("/shard/query/search", authMiddleware(searchHandler))

	go watchGateway()

	log.Printf("[slave-go] listening on %s", slavePort)
	log.Fatal(http.ListenAndServe(slavePort, mux))
}

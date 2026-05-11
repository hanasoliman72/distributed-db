package replication

// receivers.go
//
// These HTTP handlers are called by slaves when THEY originate a write.
// The master applies the change to its own MySQL, then re-broadcasts to
// all OTHER slaves (excluding the one that sent the request).
//
// Routes registered in main.go:
//   POST /replicate/query/insert
//   POST /replicate/query/update
//   POST /replicate/query/delete
//   GET  /snapshot          ← slaves pull this on startup / recovery

import (
	"encoding/json"
	"log"
	"master/storage"
	"net/http"
)

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func decode(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

// originOf reads the optional "origin" field from a request body that has
// already been decoded into a map. Returns "" if not present.
func originOf(r *http.Request) string {
	// origin is passed as a query param so we don't need to re-parse the body
	return r.URL.Query().Get("origin")
}

// BroadcastExcept broadcasts to all slaves except the one at excludeURL.
// Used by receive-handlers so the originating slave is not sent a duplicate.
func BroadcastExcept(endpoint string, payload any, excludeURL string) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[replication] marshal error: %v", err)
		return
	}
	targets := make([]*Slave, 0, len(Registry))
	for _, s := range Registry {
		if s.URL == excludeURL {
			continue // skip the originator
		}
		if s.isAlive() {
			targets = append(targets, s)
		}
	}
	if len(targets) == 0 {
		return
	}
	resultCh := make(chan BroadcastResult, len(targets))
	for _, slave := range targets {
		go func(s *Slave) { resultCh <- sendToSlave(s, endpoint, body) }(slave)
	}
	for range targets {
		r := <-resultCh
		if r.Success {
			log.Printf("[replication] ✓ %s%s", r.SlaveURL, endpoint)
		} else {
			log.Printf("[replication] ✗ %s%s  err=%s", r.SlaveURL, endpoint, r.Error)
		}
	}
	close(resultCh)
}

// ── POST /replicate/query/insert ──────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "record":{"id":1,"name":"Ali"}, "origin":"http://slave:8083" }

func ReceiveInsert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
		Origin string         `json:"origin"` // URL of the slave that sent this
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if _, err := storage.InsertRecord(req.DB, req.Table, req.Record); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Re-broadcast to all slaves EXCEPT the one that sent this request.
	go BroadcastExcept("/replicate/query/insert", map[string]any{
		"db":     req.DB,
		"table":  req.Table,
		"record": req.Record,
	}, req.Origin)

	log.Printf("[receive] insert from %s → %s.%s id=%v", req.Origin, req.DB, req.Table, req.Record["id"])
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/query/update ──────────────────────────────────────────

func ReceiveUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Where  map[string]any `json:"where"`
		Set    map[string]any `json:"set"`
		Origin string         `json:"origin"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if _, err := storage.UpdateRecords(req.DB, req.Table, req.Where, req.Set); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go BroadcastExcept("/replicate/query/update", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
		"set":   req.Set,
	}, req.Origin)

	log.Printf("[receive] update from %s → %s.%s", req.Origin, req.DB, req.Table)
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/query/delete ──────────────────────────────────────────

func ReceiveDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Where  map[string]any `json:"where"`
		Origin string         `json:"origin"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if _, err := storage.DeleteRecords(req.DB, req.Table, req.Where); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go BroadcastExcept("/replicate/query/delete", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
	}, req.Origin)

	log.Printf("[receive] delete from %s → %s.%s", req.Origin, req.DB, req.Table)
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/table/create ─────────────────────────────────────────

func ReceiveCreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
		Origin     string   `json:"origin"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if err := storage.CreateTable(req.DB, req.Table, req.Attributes); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go BroadcastExcept("/replicate/table/create", map[string]any{
		"db":         req.DB,
		"table":      req.Table,
		"attributes": req.Attributes,
	}, req.Origin)

	log.Printf("[receive] create table from %s → %s.%s", req.Origin, req.DB, req.Table)
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/table/drop ────────────────────────────────────────────

func ReceiveDropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string `json:"db"`
		Table  string `json:"table"`
		Origin string `json:"origin"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if err := storage.DropTable(req.DB, req.Table); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go BroadcastExcept("/replicate/table/drop", map[string]any{
		"db":    req.DB,
		"table": req.Table,
	}, req.Origin)

	log.Printf("[receive] drop table from %s → %s.%s", req.Origin, req.DB, req.Table)
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── GET /snapshot ─────────────────────────────────────────────────────────
// Called by a slave on startup or after recovery to get the full state.
// The master returns the same snapshot format that StartHealthChecker uses.

func ServeSnapshot(buildSnapshotFn func() map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := buildSnapshotFn()
		if snap == nil {
			respond(w, http.StatusInternalServerError, map[string]string{"error": "could not build snapshot"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
		log.Println("[snapshot] served full snapshot to slave")
	}
}

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

// ── POST /replicate/query/insert ──────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "record":{"id":1,"name":"Ali"} }
// Slave already generated the id — master inserts with the same id so all
// nodes stay on the same sequence.

func ReceiveInsert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	// Apply locally on master MySQL.
	if _, err := storage.InsertRecord(req.DB, req.Table, req.Record); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Re-broadcast to all other slaves (fire-and-forget).
	// The slave that sent this will be skipped if its URL is in the registry
	// and is currently the only alive slave — but in a multi-slave setup
	// the remaining slaves still need the write.
	go Broadcast("/replicate/query/insert", map[string]any{
		"db":     req.DB,
		"table":  req.Table,
		"record": req.Record,
	})

	log.Printf("[receive] insert from slave → %s.%s id=%v", req.DB, req.Table, req.Record["id"])
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/query/update ──────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"id":"1"}, "set":{"age":"21"} }

func ReceiveUpdate(w http.ResponseWriter, r *http.Request) {
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

	if _, err := storage.UpdateRecords(req.DB, req.Table, req.Where, req.Set); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go Broadcast("/replicate/query/update", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
		"set":   req.Set,
	})

	log.Printf("[receive] update from slave → %s.%s", req.DB, req.Table)
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/query/delete ──────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"id":"1"} }

func ReceiveDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if _, err := storage.DeleteRecords(req.DB, req.Table, req.Where); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go Broadcast("/replicate/query/delete", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
	})

	log.Printf("[receive] delete from slave → %s.%s", req.DB, req.Table)
	respond(w, http.StatusOK, map[string]string{"status": "applied"})
}

// ── POST /replicate/table/drop ────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users" }

func ReceiveDropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if err := storage.DropTable(req.DB, req.Table); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go Broadcast("/replicate/table/drop", map[string]any{
		"db":    req.DB,
		"table": req.Table,
	})

	log.Printf("[receive] drop table from slave → %s.%s", req.DB, req.Table)
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

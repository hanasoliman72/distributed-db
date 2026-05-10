package handlers

import (
	"encoding/json"
	"master/replication"
	"master/storage"
	"net/http"
)

// respond writes a JSON response with the given status code.
func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// decode reads and decodes the JSON request body into dst.
func decode(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

// ── /db/create  POST ──────────────────────────────────────────────────────
// Body: { "db": "mydb" }

func CreateDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "field 'db' is required"})
		return
	}

	if err := storage.CreateDB(req.DB); err != nil {
		respond(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}

	// ✅ Replicate: tell all slaves to create the same database
	go replication.Broadcast("/replicate/db/create", map[string]any{
		"db": req.DB,
	})

	respond(w, http.StatusCreated, map[string]string{"message": "database '" + req.DB + "' created"})
}

// ── /db/drop  DELETE ──────────────────────────────────────────────────────
// Body: { "db": "mydb" }

func DropDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "field 'db' is required"})
		return
	}

	if err := storage.DropDB(req.DB); err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	// ✅ Replicate: tell all slaves to drop the same database
	go replication.Broadcast("/replicate/db/drop", map[string]any{
		"db": req.DB,
	})

	respond(w, http.StatusOK, map[string]string{"message": "database '" + req.DB + "' dropped"})
}

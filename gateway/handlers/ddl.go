package handlers

import (
	"gateway/metadata"
	"gateway/shard"
	"net/http"
)

// ── /db/create  POST ───────────────────────────────────────────────────────
func CreateDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}

	results := shard.BroadcastAll("POST", "/shard/db/create", map[string]any{"db": req.DB})
	if len(results) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}
	for _, r := range results {
		if r.Err != nil || r.StatusCode >= 400 {
			respond(w, http.StatusInternalServerError, map[string]string{
				"error": "failed on slave " + r.SlaveID,
			})
			return
		}
	}

	metadata.SaveMetadata()
	respond(w, http.StatusCreated, map[string]string{"message": "database '" + req.DB + "' created on all shards"})
}

// ── /db/drop  DELETE ───────────────────────────────────────────────────────
func DropDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}

	for _, m := range metadata.TablesInDB(req.DB) {
		metadata.DropTableMeta(m.DB, m.Table)
	}

	results := shard.BroadcastAll("DELETE", "/shard/db/drop", map[string]any{"db": req.DB})
	if len(results) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}

	metadata.SaveMetadata()
	respond(w, http.StatusOK, map[string]string{"message": "database '" + req.DB + "' dropped from all shards"})
}

// ── /table/create  POST ────────────────────────────────────────────────────
func CreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || len(req.Attributes) == 0 {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'attributes' are required"})
		return
	}

	meta := metadata.CreateTableMeta(req.DB, req.Table, req.Attributes)
	if meta.ShardCount == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available to host the table"})
		return
	}

	results := shard.BroadcastAll("POST", "/shard/table/create", map[string]any{
		"db":         req.DB,
		"table":      req.Table,
		"attributes": req.Attributes,
	})
	for _, res := range results {
		if res.Err != nil || res.StatusCode >= 400 {
			respond(w, http.StatusInternalServerError, map[string]string{
				"error": "create table failed on slave " + res.SlaveID,
			})
			return
		}
	}

	metadata.SaveMetadata()
	respond(w, http.StatusCreated, map[string]any{
		"message":     "table '" + req.Table + "' created",
		"shard_count": meta.ShardCount,
		"shards":      meta.SlaveIDs,
	})
}

// ── /table/drop  DELETE ────────────────────────────────────────────────────
func DropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' and 'table' are required"})
		return
	}

	metadata.DropTableMeta(req.DB, req.Table)

	results := shard.BroadcastAll("DELETE", "/shard/table/drop", map[string]any{
		"db":    req.DB,
		"table": req.Table,
	})
	if len(results) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}

	metadata.SaveMetadata()

	respond(w, http.StatusOK, map[string]string{"message": "table '" + req.Table + "' dropped"})
}

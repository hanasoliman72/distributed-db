package handlers

import (
	"gateway/metadata"
	"net/http"
)

func CreateDB(w http.ResponseWriter, r *http.Request) {
	var req struct{ DB string `json:"db"` }
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}
	if !broadcast(w, "POST", "/shard/db/create", map[string]any{"db": req.DB}) {
		return
	}
	metadata.ClearDroppedDB(req.DB)
	metadata.BumpVersion()
	metadata.SaveMetadata()
	metadata.ReplicateToSlaves()
	respond(w, http.StatusCreated, map[string]string{"message": "database '" + req.DB + "' created on all shards"})
}

func DropDB(w http.ResponseWriter, r *http.Request) {
	var req struct{ DB string `json:"db"` }
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}
	if !broadcast(w, "DELETE", "/shard/db/drop", map[string]any{"db": req.DB}) {
		return
	}
	metadata.MarkDBDropped(req.DB)
	respond(w, http.StatusOK, map[string]string{"message": "database '" + req.DB + "' dropped; shard routing retained for replica fallback"})
}

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
	if !broadcast(w, "POST", "/shard/table/create", map[string]any{"db": req.DB, "table": req.Table, "attributes": req.Attributes}) {
		return
	}
	metadata.SaveMetadata()
	respond(w, http.StatusCreated, map[string]any{"message": "table '" + req.Table + "' created", "shard_count": meta.ShardCount, "shards": meta.SlaveIDs})
}

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
	if !broadcast(w, "DELETE", "/shard/table/drop", map[string]any{"db": req.DB, "table": req.Table}) {
		return
	}
	metadata.SaveMetadata()
	respond(w, http.StatusOK, map[string]string{"message": "table '" + req.Table + "' dropped"})
}

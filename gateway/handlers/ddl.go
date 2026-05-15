package handlers

// ddl.go
//
// Handlers for schema-management (DDL) operations.
//
// DB-DROP RESILIENCE NOTE
// ───────────────────────
// DropDB does NOT remove table metadata from the shard map.  The slave
// storage layer already transparently falls back to the <db>_replica schema
// on every read and write, so queries keep working even after the primary DB
// is gone.  Keeping the TableMeta entries means the gateway still knows which
// slave owns each shard and can route traffic correctly.
//
// Only /table/drop explicitly removes a TableMeta entry.

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

	// Remove from dropped-DB set so future inserts route to the primary again.
	// BumpVersion + SaveMetadata + ReplicateToSlaves pushes the cleaned snapshot
	// to every slave so they also clear their local replica-only mode.
	metadata.ClearDroppedDB(req.DB)
	metadata.BumpVersion()
	metadata.SaveMetadata()
	metadata.ReplicateToSlaves()

	respond(w, http.StatusCreated, map[string]string{
		"message": "database '" + req.DB + "' created on all shards",
	})
}

// ── /db/drop  DELETE ───────────────────────────────────────────────────────
//
// The primary DB is dropped from every slave but the shard map (TableMeta) is
// intentionally kept intact.  This means:
//   • Subsequent SELECT / INSERT / UPDATE / DELETE calls are still routed to
//     the correct shards.
//   • Each slave's storage layer will transparently use <db>_replica for
//     every operation (storage.go already implements this).
//   • If the caller later re-creates the DB, inserts will go to the primary
//     again and the replica will stay in sync as usual.

func DropDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB string `json:"db"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' is required"})
		return
	}

	// Broadcast the physical DROP to every slave (primary + replica are both
	// dropped on the slave side).
	results := shard.BroadcastAll("DELETE", "/shard/db/drop", map[string]any{"db": req.DB})
	if len(results) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}

	// ── KEY CHANGE: do NOT remove TableMeta entries ───────────────────────
	// MarkDBDropped bumps the version and replicates so every node knows the
	// primary is gone, but leaves the shard map intact.
	metadata.MarkDBDropped(req.DB)

	respond(w, http.StatusOK, map[string]string{
		"message": "database '" + req.DB + "' dropped; shard routing retained for replica fallback",
	})
}

// ── /table/create  POST ────────────────────────────────────────────────────

func CreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || len(req.Attributes) == 0 {
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "'db', 'table', and 'attributes' are required",
		})
		return
	}

	meta := metadata.CreateTableMeta(req.DB, req.Table, req.Attributes)
	if meta.ShardCount == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{
			"error": "no slaves available to host the table",
		})
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
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "'db' and 'table' are required",
		})
		return
	}

	// Remove the shard-map entry first so no new queries can arrive.
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
	respond(w, http.StatusOK, map[string]string{
		"message": "table '" + req.Table + "' dropped",
	})
}

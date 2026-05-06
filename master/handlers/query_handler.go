package handlers

import (
	"master/replication"
	"master/storage"
	"net/http"
)

// ── /query/insert  POST ───────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "record":{"id":1,"name":"Ali","age":20} }

func Insert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "fields 'db', 'table', and 'record' are required"})
		return
	}

	if err := storage.InsertRecord(req.DB, req.Table, req.Record); err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go replication.Broadcast("/replicate/query/insert", map[string]any{
		"db":     req.DB,
		"table":  req.Table,
		"record": req.Record,
	})

	respond(w, http.StatusCreated, map[string]string{"message": "record inserted"})
}

// ── /query/select  POST ───────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"name":"Ali"} }
// Omit or leave "where" empty to select all records.

func Select(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "fields 'db' and 'table' are required"})
		return
	}

	records, err := storage.SelectRecords(req.DB, req.Table, req.Where)
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	// Return empty array instead of null when no records match
	if records == nil {
		records = []map[string]any{}
	}

	respond(w, http.StatusOK, map[string]any{
		"count":   len(records),
		"records": records,
	})
}

// ── /query/update  PUT ────────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"name":"Ali"}, "set":{"age":21} }

func Update(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Set == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "fields 'db', 'table', 'where', and 'set' are required"})
		return
	}

	count, err := storage.UpdateRecords(req.DB, req.Table, req.Where, req.Set)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go replication.Broadcast("/replicate/query/update", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
		"set":   req.Set,
	})

	respond(w, http.StatusOK, map[string]any{
		"message":         "update complete",
		"records_updated": count,
	})
}

// ── /query/delete  DELETE ─────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"name":"Ali"} }

func Delete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "fields 'db', 'table', and 'where' are required"})
		return
	}

	count, err := storage.DeleteRecords(req.DB, req.Table, req.Where)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go replication.Broadcast("/replicate/query/delete", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
	})

	respond(w, http.StatusOK, map[string]any{
		"message":         "delete complete",
		"records_deleted": count,
	})
}

package handlers

import (
	"master/replication"
	"master/storage"
	"net/http"
)

func CreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB         string   `json:"db"`
		Table      string   `json:"table"`
		Attributes []string `json:"attributes"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "fields 'db', 'table', and 'attributes' are required"})
		return
	}
	if err := storage.CreateTable(req.DB, req.Table, req.Attributes); err != nil {
		respond(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	go replication.Broadcast("/replicate/table/create", map[string]any{
		"db": req.DB, "table": req.Table, "attributes": req.Attributes,
	})
	respond(w, http.StatusCreated, map[string]string{
		"message": "table '" + req.Table + "' created in database '" + req.DB + "'",
	})
}

func DropTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "fields 'db' and 'table' are required"})
		return
	}
	if err := storage.DropTable(req.DB, req.Table); err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	go replication.Broadcast("/replicate/table/drop", map[string]any{
		"db": req.DB, "table": req.Table,
	})
	respond(w, http.StatusOK, map[string]string{
		"message": "table '" + req.Table + "' dropped from database '" + req.DB + "'",
	})
}

func ListTables(w http.ResponseWriter, r *http.Request) {
	db := r.URL.Query().Get("db")
	if db == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "query param 'db' is required"})
		return
	}
	tables, err := storage.ListTables(db)
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]any{"tables": tables})
}

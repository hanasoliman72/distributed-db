package handlers

import (
	"fmt"
	"master/replication"
	"master/storage"
	"net/http"
	"strings"
)

// ── /query/insert  POST ───────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "record":{"name":"Ali","age":"20"} }
//
// Do NOT include "id" – MySQL generates it automatically.
// The response includes the generated id.

func Insert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "fields 'db', 'table', and 'record' are required. Do not include 'id' – it is auto-generated.",
		})
		return
	}

	// Call InsertRecord directly in its own goroutine so we get the
	// generated id back through a typed channel (WriteQueue only returns error).
	type insertResult struct {
		id  int64
		err error
	}
	resultCh := make(chan insertResult, 1)
	go func() {
		id, err := storage.InsertRecord(req.DB, req.Table, req.Record)
		resultCh <- insertResult{id: id, err: err}
	}()

	res := <-resultCh
	if res.err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": res.err.Error()})
		return
	}

	// Broadcast to slaves – include the generated id so slaves store the
	// exact same row with the same id.
	recordWithID := make(map[string]any, len(req.Record)+1)
	for k, v := range req.Record {
		recordWithID[k] = v
	}
	recordWithID["id"] = res.id

	go replication.Broadcast("/replicate/query/insert", map[string]any{
		"db":     req.DB,
		"table":  req.Table,
		"record": recordWithID,
	})

	respond(w, http.StatusCreated, map[string]any{
		"message":      "record inserted",
		"generated_id": res.id,
	})
}

// ── /query/select  GET ────────────────────────────────────────────────────
// Filters are passed as query parameters (GET has no body).
//
// Select ALL records:
//   GET /query/select?db=mydb&table=users
//
// Select by id:
//   GET /query/select?db=mydb&table=users&id=3
//
// Select by name:
//   GET /query/select?db=mydb&table=users&name=Ali
//
// Select by age:
//   GET /query/select?db=mydb&table=users&age=20
//
// Select by multiple attributes (AND logic):
//   GET /query/select?db=mydb&table=users&name=Ali&age=20

func Select(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	db := q.Get("db")
	table := q.Get("table")
	if db == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "query params 'db' and 'table' are required",
		})
		return
	}

	// Every query param that is NOT "db" or "table" becomes a WHERE condition.
	where := map[string]any{}
	for key, vals := range q {
		if key == "db" || key == "table" {
			continue
		}
		where[key] = vals[0] // first value of each param
	}

	records, err := storage.SelectRecords(db, table, where)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if records == nil {
		records = []map[string]any{}
	}

	respond(w, http.StatusOK, map[string]any{
		"count":   len(records),
		"records": records,
	})
}

// ── /query/update  PUT ────────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"id":"3"}, "set":{"age":"21"} }

func Update(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Set == nil {
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "fields 'db', 'table', 'where', and 'set' are required",
		})
		return
	}

	resultCh := make(chan error, 1)
	replication.WriteQueue <- replication.WriteJob{
		Operation: "update",
		DB:        req.DB,
		Table:     req.Table,
		Where:     req.Where,
		Set:       req.Set,
		ResultCh:  resultCh,
	}
	if err := <-resultCh; err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go replication.Broadcast("/replicate/query/update", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
		"set":   req.Set,
	})

	respond(w, http.StatusOK, map[string]string{"message": "update complete"})
}

// ── /query/delete  DELETE ─────────────────────────────────────────────────
// Body: { "db":"mydb", "table":"users", "where":{"id":"3"} }

func Delete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{
			"error": "fields 'db', 'table', and 'where' are required",
		})
		return
	}

	resultCh := make(chan error, 1)
	replication.WriteQueue <- replication.WriteJob{
		Operation: "delete",
		DB:        req.DB,
		Table:     req.Table,
		Where:     req.Where,
		ResultCh:  resultCh,
	}
	if err := <-resultCh; err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	go replication.Broadcast("/replicate/query/delete", map[string]any{
		"db":    req.DB,
		"table": req.Table,
		"where": req.Where,
	})

	respond(w, http.StatusOK, map[string]string{"message": "delete complete"})
}

// GET /query/search?db=mydb&table=users&q=ali
func Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db := q.Get("db")
	table := q.Get("table")
	term := strings.TrimSpace(q.Get("q"))

	if db == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' and 'table' are required"})
		return
	}
	if term == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'q' (search term) is required"})
		return
	}

	all, err := storage.SelectRecords(db, table, map[string]any{})
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

	respond(w, http.StatusOK, map[string]any{
		"search_term": term,
		"count":       len(matched),
		"records":     matched,
	})
}

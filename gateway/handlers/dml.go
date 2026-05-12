package handlers

// dml.go
//
// Handlers for data-manipulation operations.
//
// INSERT  → route to one shard (round-robin among alive slaves)
// SELECT  → fan-out to ALL shards, merge results via MapReducer
// UPDATE  → route to the shard that owns the id (or all if no id filter)
// DELETE  → same routing logic as UPDATE
// SEARCH  → fan-out to ALL shards, merge via MapReducer

import (
	"fmt"
	"gateway/metadata"
	"gateway/shard"
	"net/http"
	"net/url"
)

// ── /query/insert  POST ────────────────────────────────────────────────────

func Insert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string         `json:"db"`
		Table  string         `json:"table"`
		Record map[string]any `json:"record"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Record == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'record' are required"})
		return
	}

	meta := metadata.GetTableMeta(req.DB, req.Table)
	if meta == nil {
		respond(w, http.StatusNotFound, map[string]string{"error": "table not found in metadata; create it first"})
		return
	}

	target, shardIdx, err := meta.NextInsertSlave()
	if err != nil {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	res := shard.Forward(target, "POST", "/shard/query/insert", map[string]any{
		"db":        req.DB,
		"table":     req.Table,
		"record":    req.Record,
		"shard_idx": shardIdx,
	})
	if res.Err != nil {
		respond(w, http.StatusBadGateway, map[string]string{"error": res.Err.Error()})
		return
	}

	res.Body["shard"] = target.ID
	respond(w, res.StatusCode, res.Body)
}

// ── /query/select  GET ─────────────────────────────────────────────────────

func Select(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db := q.Get("db")
	table := q.Get("table")
	if db == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' and 'table' are required"})
		return
	}

	// If a specific id is requested, route to the owning shard.
	if idVal := q.Get("id"); idVal != "" {
		meta := metadata.GetTableMeta(db, table)
		if meta != nil {
			if target := meta.SlaveForID(idVal); target != nil && target.IsAlive() {
				path := "/shard/query/select?" + q.Encode()
				res := shard.ForwardGet(target, path)
				if res.Err == nil {
					respond(w, res.StatusCode, res.Body)
					return
				}
				// fall through to fan-out on error
			}
		}
	}

	// Fan-out to all shards and merge.
	alive := metadata.AliveSlaves()
	if len(alive) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}

	type shardResult struct {
		records []any
		err     error
	}
	ch := make(chan shardResult, len(alive))
	path := "/shard/query/select?" + q.Encode()

	for _, s := range alive {
		go func(sl *metadata.Slave) {
			res := shard.ForwardGet(sl, path)
			if res.Err != nil {
				ch <- shardResult{err: res.Err}
				return
			}
			rows, _ := res.Body["records"].([]any)
			ch <- shardResult{records: rows}
		}(s)
	}

	merged := make([]any, 0)
	for range alive {
		sr := <-ch
		if sr.err == nil {
			merged = append(merged, sr.records...)
		}
	}
	close(ch)

	respond(w, http.StatusOK, map[string]any{"count": len(merged), "records": merged})
}

// ── /query/update  PUT ─────────────────────────────────────────────────────

func Update(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
		Set   map[string]any `json:"set"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" || req.Set == nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', 'where', and 'set' are required"})
		return
	}

	targets := routeWriteTargets(req.DB, req.Table, req.Where)
	if len(targets) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}

	payload := map[string]any{"db": req.DB, "table": req.Table, "where": req.Where, "set": req.Set}
	totalAffected := 0
	for _, target := range targets {
		res := shard.Forward(target, "PUT", "/shard/query/update", payload)
		if res.Err == nil {
			if n, ok := res.Body["records_updated"].(float64); ok {
				totalAffected += int(n)
			}
		}
	}
	respond(w, http.StatusOK, map[string]any{"message": "update complete", "records_updated": totalAffected})
}

// ── /query/delete  DELETE ──────────────────────────────────────────────────

func Delete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string         `json:"db"`
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := decode(r, &req); err != nil || req.DB == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'where' are required"})
		return
	}

	targets := routeWriteTargets(req.DB, req.Table, req.Where)
	if len(targets) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}

	payload := map[string]any{"db": req.DB, "table": req.Table, "where": req.Where}
	totalAffected := 0
	for _, target := range targets {
		res := shard.Forward(target, "DELETE", "/shard/query/delete", payload)
		if res.Err == nil {
			if n, ok := res.Body["records_deleted"].(float64); ok {
				totalAffected += int(n)
			}
		}
	}
	respond(w, http.StatusOK, map[string]any{"message": "delete complete", "records_deleted": totalAffected})
}

// ── /query/search  GET ─────────────────────────────────────────────────────

func Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db := q.Get("db")
	table := q.Get("table")
	term := q.Get("q")
	if db == "" || table == "" || term == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'q' are required"})
		return
	}

	alive := metadata.AliveSlaves()
	ch := make(chan []any, len(alive))
	path := fmt.Sprintf("/shard/query/search?db=%s&table=%s&q=%s", db, table, url.QueryEscape(term))

	for _, s := range alive {
		go func(sl *metadata.Slave) {
			res := shard.ForwardGet(sl, path)
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
}

// ── helpers ────────────────────────────────────────────────────────────────

// routeWriteTargets returns the slave(s) that should receive a write.
// If "id" is in the where clause, route to the owning shard only.
// Otherwise broadcast to all alive slaves.
func routeWriteTargets(db, table string, where map[string]any) []*metadata.Slave {
	if idVal, ok := where["id"]; ok {
		meta := metadata.GetTableMeta(db, table)
		if meta != nil {
			if target := meta.SlaveForID(fmt.Sprintf("%v", idVal)); target != nil && target.IsAlive() {
				return []*metadata.Slave{target}
			}
		}
	}
	return metadata.AliveSlaves()
}

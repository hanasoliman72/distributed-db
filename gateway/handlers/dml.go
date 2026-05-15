package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gateway/metadata"
	"gateway/shard"
	"io"
	"net/http"
	"net/url"
	"time"
)

var (
	mapReducerURL = "http://127.0.0.1:8090"
	mrClient      = &http.Client{Timeout: 10 * time.Second}
)

func callMapReducer(shards [][]any, orderBy, order string, limit int) ([]any, error) {
	body, _ := json.Marshal(map[string]any{"shards": shards, "order_by": orderBy, "order": order, "limit": limit})
	resp, err := mrClient.Post(mapReducerURL+"/reduce", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("mapReducer unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("mapReducer error: status=%d body=%s", resp.StatusCode, string(raw))
	}
	var result struct{ Records []any `json:"records"` }
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("mapReducer: bad response: %w", err)
	}
	return result.Records, nil
}

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
		"db": req.DB, "table": req.Table, "record": req.Record, "shard_idx": shardIdx,
	})
	if res.Err != nil {
		respond(w, http.StatusBadGateway, map[string]string{"error": res.Err.Error()})
		return
	}
	res.Body["shard"] = target.ID
	respond(w, res.StatusCode, res.Body)
}

func Select(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db, table := q.Get("db"), q.Get("table")
	if db == "" || table == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db' and 'table' are required"})
		return
	}
	// Fast path: known id → route to its owning shard only.
	if idVal := q.Get("id"); idVal != "" {
		if meta := metadata.GetTableMeta(db, table); meta != nil {
			if target := meta.SlaveForID(idVal); target != nil && target.IsAlive() {
				if res := shard.ForwardGet(target, "/shard/query/select?"+q.Encode()); res.Err == nil {
					respond(w, res.StatusCode, res.Body)
					return
				}
			}
		}
	}
	alive := metadata.AliveSlaves()
	if len(alive) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}
	limit := 0
	fmt.Sscanf(q.Get("limit"), "%d", &limit)
	merged, err := callMapReducer(fanOutRows(alive, "/shard/query/select?"+q.Encode()), q.Get("order_by"), q.Get("order"), limit)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]any{"count": len(merged), "records": merged})
}

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
	n := writeToTargets(targets, "PUT", "/shard/query/update",
		map[string]any{"db": req.DB, "table": req.Table, "where": req.Where, "set": req.Set}, "records_updated")
	respond(w, http.StatusOK, map[string]any{"message": "update complete", "records_updated": n})
}

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
	n := writeToTargets(targets, "DELETE", "/shard/query/delete",
		map[string]any{"db": req.DB, "table": req.Table, "where": req.Where}, "records_deleted")
	respond(w, http.StatusOK, map[string]any{"message": "delete complete", "records_deleted": n})
}

func Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db, table, term := q.Get("db"), q.Get("table"), q.Get("q")
	if db == "" || table == "" || term == "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "'db', 'table', and 'q' are required"})
		return
	}
	alive := metadata.AliveSlaves()
	if len(alive) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return
	}
	path := fmt.Sprintf("/shard/query/search?db=%s&table=%s&q=%s", url.QueryEscape(db), url.QueryEscape(table), url.QueryEscape(term))
	merged, err := callMapReducer(fanOutRows(alive, path), "", "", 0)
	if err != nil {
		respond(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respond(w, http.StatusOK, map[string]any{"search_term": term, "count": len(merged), "records": merged})
}

func routeWriteTargets(db, table string, where map[string]any) []*metadata.Slave {
	if idVal, ok := where["id"]; ok {
		if meta := metadata.GetTableMeta(db, table); meta != nil {
			if target := meta.SlaveForID(fmt.Sprintf("%v", idVal)); target != nil && target.IsAlive() {
				return []*metadata.Slave{target}
			}
		}
	}
	return metadata.AliveSlaves()
}

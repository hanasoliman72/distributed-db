package handlers

import (
	"encoding/json"
	"gateway/metadata"
	"gateway/shard"
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

func broadcast(w http.ResponseWriter, method, endpoint string, payload any) bool {
	results := shard.BroadcastAll(method, endpoint, payload)
	if len(results) == 0 {
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "no slaves available"})
		return false
	}
	for _, r := range results {
		if r.Err != nil || r.StatusCode >= 400 {
			respond(w, http.StatusInternalServerError, map[string]string{"error": "failed on slave " + r.SlaveID})
			return false
		}
	}
	return true
}

func fanOutRows(alive []*metadata.Slave, path string) [][]any {
	ch := make(chan []any, len(alive))
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
	buckets := make([][]any, 0, len(alive))
	for range alive {
		if rows := <-ch; rows != nil {
			buckets = append(buckets, rows)
		}
	}
	close(ch)
	return buckets
}

func writeToTargets(targets []*metadata.Slave, method, endpoint string, payload any, countKey string) int {
	ch := make(chan int, len(targets))
	for _, t := range targets {
		go func(sl *metadata.Slave) {
			n := 0
			if res := shard.Forward(sl, method, endpoint, payload); res.Err == nil {
				if v, ok := res.Body[countKey].(float64); ok {
					n = int(v)
				}
			}
			ch <- n
		}(t)
	}
	total := 0
	for range targets {
		total += <-ch
	}
	close(ch)
	return total
}

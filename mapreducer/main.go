package main

// mapreducer/main.go
//
// The MapReducer is a lightweight HTTP service that the gateway calls when it
// needs to merge results from multiple shards (SELECT fan-out, SEARCH).
//
// It is intentionally stateless: it receives a batch of row lists, applies
// optional filters / ordering, and returns the merged set.
//
// POST /reduce
// Body:
//   {
//     "shards":  [ [ {row}, {row} ], [ {row} ], … ],   // one array per shard
//     "order_by": "age",                                 // optional
//     "order":    "asc" | "desc",                        // optional, default "asc"
//     "limit":    100                                     // optional, 0 = no limit
//   }
// Response:
//   { "count": N, "records": [ … ] }
//
// The gateway calls this service transparently — clients never see it.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
)

const defaultPort = ":8090"

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/reduce", reduceHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "role": "mapreducer"})
	})

	port := envOr("MAPREDUCER_PORT", defaultPort)
	log.Printf("[mapreducer] listening on %s", port)
	log.Fatal(http.ListenAndServe(port, mux))
}

// ── /reduce  POST ──────────────────────────────────────────────────────────

func reduceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Shards  [][]map[string]any `json:"shards"`
		OrderBy string             `json:"order_by"`
		Order   string             `json:"order"`
		Limit   int                `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	// MAP: flatten all shard results into one slice.
	merged := make([]map[string]any, 0)
	for _, shardRows := range req.Shards {
		merged = append(merged, shardRows...)
	}

	// REDUCE: optional sort.
	if req.OrderBy != "" {
		desc := req.Order == "desc"
		sort.SliceStable(merged, func(i, j int) bool {
			vi := fmt.Sprintf("%v", merged[i][req.OrderBy])
			vj := fmt.Sprintf("%v", merged[j][req.OrderBy])
			// Try numeric comparison first.
			fi, erri := strconv.ParseFloat(vi, 64)
			fj, errj := strconv.ParseFloat(vj, 64)
			if erri == nil && errj == nil {
				if desc {
					return fi > fj
				}
				return fi < fj
			}
			if desc {
				return vi > vj
			}
			return vi < vj
		})
	}

	// REDUCE: optional limit.
	if req.Limit > 0 && len(merged) > req.Limit {
		merged = merged[:req.Limit]
	}

	respond(w, http.StatusOK, map[string]any{"count": len(merged), "records": merged})
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

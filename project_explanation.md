# Distributed Database — Full Explanation

---

## 1. Architecture Overview (Big Picture)

This is a **Federated Distributed Database** system. Instead of one big database, data is split across multiple machines called **slaves**, and a central **Gateway** coordinates everything.

```
                        ┌─────────────────────────────────┐
         CLIENT         │          GATEWAY  (:8080)        │
    (Postman / App)  →  │  Routes, metadata, health check  │
                        └───────────┬─────────────────────┘
                                    │ broadcasts / forwards
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             slave-a (:8081)  slave-b (:8082)  slave-c (:8083)
              [Go + MySQL]    [Python + MySQL]  [.NET + MySQL]
                    │               │               │
             [MapReducer (:8090)] ◄─┘───────────────┘
               merges results from all shards
```

### Ports at a glance

| Service     | Port  | Language |
|-------------|-------|----------|
| Gateway     | 8080  | Go       |
| slave-a     | 8081  | Go       |
| slave-b     | 8082  | Python   |
| slave-c     | 8083  | .NET/C#  |
| MapReducer  | 8090  | Go       |

---

## 2. Terminology Explained

### 🔷 Gateway
The **single entry point** for all client requests. It does NOT store any data itself. It:
- Knows which slaves exist and which are alive
- Routes every request to the right slave(s)
- Keeps a **metadata** map (which table lives on which slaves)
- Performs **health checks** on slaves every 10 seconds

### 🔷 Slave
A **worker node** that actually stores data in MySQL. There are 3 slaves, each running a different tech stack (Go, Python, .NET) but they all do the **same job** — they expose the same HTTP API (`/shard/...` endpoints) and store data in MySQL.

### 🔷 Shard
A **shard** is a partition (slice) of a table's data. Instead of all rows living in one place, rows are **distributed** across slaves. For example, if you have 100 rows in a `users` table and 3 slaves, roughly:
- slave-a holds rows 1, 4, 7, 10 ... (inserted in round-robin)
- slave-b holds rows 2, 5, 8, 11 ...
- slave-c holds rows 3, 6, 9, 12 ...

The `shard` **package** (`gateway/shard/shared.go`) is the Go code responsible for forwarding HTTP requests to slaves and collecting results.

**ShardCount** = how many slaves a table is spread across (= number of alive slaves at table creation time).

**SlaveIDs** = which specific slaves hold this table's shards. E.g. `["slave-a", "slave-b", "slave-c"]`.

### 🔷 Metadata
A **JSON file** (`metadata.json`) that acts as the brain's memory. It stores:
- List of all slaves and their URLs
- For every table: which slaves hold it, how many shards, what columns it has
- `dropped_dbs`: list of databases that have been deleted
- `version`: incremented every time something changes

Both the Gateway and each slave have a copy of `metadata.json`. The Gateway is the source of truth and **replicates** it to all slaves after every change.

### 🔷 Snapshot
A `Snapshot` is the full serialized state of the metadata at a point in time — version + slaves + tables + dropped_dbs. Used to replicate state and recover from failures.

### 🔷 MapReducer
A **separate microservice** (`:8090`) that merges results from multiple shards. When a SELECT query goes to all 3 slaves, each slave returns its rows. The MapReducer:
1. **Merges** all rows into one list
2. **Sorts** them (if `order_by` is specified)
3. **Limits** the results (if `limit` is specified)

This is the "Reduce" step of the **MapReduce** pattern — the Map step is the parallel fan-out to slaves.

### 🔷 DDL vs DML
- **DDL** (Data Definition Language): `CREATE DATABASE`, `DROP DATABASE`, `CREATE TABLE`, `DROP TABLE` — structure operations
- **DML** (Data Manipulation Language): `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `SEARCH` — data operations

### 🔷 Replica (inside each slave)
Each slave automatically keeps a **local backup** called `{db}_replica`. For example if your database is called `UNI`, the slave also maintains `UNI_replica`. On every INSERT/UPDATE/DELETE, the slave writes to both the main DB and the replica. On SELECT, if the main DB fails, it falls back to the replica.

### 🔷 HMAC Token / Auth
Every request the Gateway sends to a slave must include a secret token in the HTTP header `X-Gateway-Token`. The token is: `nonce|HMAC_SHA256(nonce, secret)`. The shared secret is `"Hana-1234"`. Slaves verify this token and reject requests without it. This prevents random clients from directly hitting the slave APIs.

---

## 3. Full Workflow — Step by Step

### Step 1: Startup (Gateway `main.go`)
```
1. Load HMAC secret ("Hana-1234")
2. Register 3 slaves in the registry (slave-a, slave-b, slave-c)
3. Load metadata.json from disk (restores previous state)
4. Start Health Checker goroutine (pings slaves every 10s)
5. Replicate current metadata snapshot to all slaves
6. Start HTTP server on :8080
```

---

### Step 2: Client creates a Database — `POST /db/create`
```
Client → Gateway /db/create {"db": "UNI"}
   Gateway Handler (CreateDB):
      1. Calls broadcast() → shard.BroadcastAll()
         → goroutine for slave-a: POST /shard/db/create {"db":"UNI"}
         → goroutine for slave-b: POST /shard/db/create {"db":"UNI"}   (all parallel!)
         → goroutine for slave-c: POST /shard/db/create {"db":"UNI"}
         → wait for all 3 responses via channel
      2. Each slave creates MySQL database "UNI" and "UNI_replica"
      3. Gateway clears "UNI" from dropped_dbs, bumps version, saves metadata.json
      4. Replicates updated metadata.json to all slaves
      5. Returns {"message": "database 'UNI' created on all shards"}
```

---

### Step 3: Client creates a Table — `POST /table/create`
```
Client → Gateway /table/create {"db":"UNI","table":"users","attributes":["name","email"]}
   Gateway Handler (CreateTable):
      1. CreateTableMeta():
         - Finds all alive slaves: [slave-a, slave-b, slave-c]
         - Creates TableMeta: {DB:"UNI", Table:"users", ShardCount:3, SlaveIDs:["slave-a","slave-b","slave-c"]}
         - Saves to in-memory map (tables["UNI.users"])
         - Bumps version, saves metadata.json, starts goroutine to replicate
      2. Broadcasts CREATE TABLE to all slaves (parallel goroutines)
      3. Each slave creates: CREATE TABLE UNI.users (id INT AUTO_INCREMENT, name TEXT, email TEXT)
      4. Returns {"message":"table 'users' created","shard_count":3,"shards":["slave-a","slave-b","slave-c"]}
```

---

### Step 4: Client inserts a row — `POST /query/insert`
```
Client → Gateway /query/insert {"db":"UNI","table":"users","record":{"name":"Ali"}}
   Gateway Handler (Insert):
      1. GetTableMeta("UNI","users") → finds TableMeta
      2. NextInsertSlave() → Round-Robin: picks slave-a (then slave-b next time, etc.)
      3. shard.Forward(slave-a, "POST", "/shard/query/insert", {..., "shard_idx":0})
         - Attaches HMAC token to HTTP header
         - slave-a receives it, verifies token, inserts into MySQL UNI.users
         - slave-a also mirrors the row to UNI_replica.users
         - Returns {"message":"record inserted","generated_id":7}
      4. Gateway returns {"message":"record inserted","generated_id":7,"shard":"slave-a"}
```

---

### Step 5: Client selects rows — `GET /query/select?db=UNI&table=users`
```
Client → Gateway /query/select?db=UNI&table=users

   CASE A — "id" is specified in the query (e.g. ?id=7):
      Gateway looks up which slave owns id=7 using FNV hash:
         hash("7") % ShardCount = index → maps to a specific slave
      Forwards GET to that ONE slave only (fast path)
      Returns that slave's result directly

   CASE B — no "id" (full scan):
      Gateway calls fanOutRows() → parallel GET to ALL alive slaves
         → goroutine for slave-a → GET /shard/query/select?db=UNI&table=users
         → goroutine for slave-b → GET /shard/query/select?db=UNI&table=users
         → goroutine for slave-c → GET /shard/query/select?db=UNI&table=users
         → all results collected via channel
      Gateway sends all shards' results to MapReducer:
         POST http://127.0.0.1:8090/reduce {"shards":[[...],[...],[...]],"order_by":"","limit":0}
      MapReducer merges + sorts + limits → returns combined list
      Gateway returns {"count":N,"records":[...]}
```

---

### Step 6: Client updates rows — `PUT /query/update`
```
Client → Gateway /query/update {"db":"UNI","table":"users","where":{"id":7},"set":{"name":"Sara"}}
   Gateway Handler (Update):
      routeWriteTargets():
         - "id" is in where → hash("7") % 3 = specific slave → update only THAT slave
         - no "id" → update ALL alive slaves
      writeToTargets() → goroutines per target → parallel PUT
      Sums up total records_updated from all responses
      Returns {"message":"update complete","records_updated":1}
```

---

### Step 7: Gateway Failure & slave-go Promotion
```
1. slave-go's watchGateway() goroutine pings /health every 3 seconds
2. If gateway misses 3 pings in a row:
      log "gateway is DOWN — promoting self"
      promote() is called:
         - Sets isGateway = 1 (atomic flag)
         - Loads metadata.json (from local file or fallback to ../gateway/)
         - Starts promoHealthChecker goroutine (monitors slaves)
         - Starts demotionChecker goroutine (watches for original gateway to come back)
         - Starts a NEW HTTP server on :8080 (takes over the gateway port)
         - Now slave-a acts as BOTH slave (:8081) and gateway (:8080)

3. When original gateway comes back online:
      demotionChecker() detects /health returns 200
      demote() is called:
         - Sets isGateway = 0
         - syncMetadataToGateway() → POSTs its current metadata.json to /gateway/sync
         - Shuts down the :8080 server
         - Restarts watchGateway() goroutine (back to slave-only mode)

4. Original gateway's /gateway/sync handler:
      Receives the snapshot from the promoted slave
      LoadMetadataFromBytes() → adopts it if version is newer
      SaveMetadata() → persists to disk
      ReplicateToSlaves() → pushes to all slaves
```

---

## 4. Goroutines & Channels — Complete List

> A **goroutine** is a lightweight concurrent thread in Go (`go func(){}`).  
> A **channel** (`chan`) is a pipe used to send data between goroutines safely.

---

### 4.1 `BroadcastAll()` — `gateway/shard/shared.go:88`
**Used for**: DDL operations (create/drop DB and table) that must happen on ALL slaves simultaneously.

```go
func BroadcastAll(method, endpoint string, payload any) []Result {
    alive := metadata.AliveSlaves()
    ch := make(chan Result, len(alive))   // ← BUFFERED channel, size = num slaves
    for _, s := range alive {
        go func(sl *metadata.Slave) {    // ← GOROUTINE per slave (parallel HTTP call)
            ch <- Forward(sl, method, endpoint, payload)
        }(s)
    }
    results := make([]Result, 0, len(alive))
    for range alive {
        results = append(results, <-ch)  // ← collect all results
    }
    close(ch)
    return results
}
```
**Why buffered?** `make(chan Result, len(alive))` — each goroutine can send without blocking, so no goroutine leaks.

---

### 4.2 `fanOutRows()` — `gateway/handlers/helpers.go:35`
**Used for**: SELECT and SEARCH — scatter a read query to all slaves, collect rows.

```go
func fanOutRows(alive []*metadata.Slave, path string) [][]any {
    ch := make(chan []any, len(alive))   // ← channel of row-slices
    for _, s := range alive {
        go func(sl *metadata.Slave) {    // ← GOROUTINE per slave
            res := shard.ForwardGet(sl, path)
            if res.Err != nil || res.StatusCode >= 400 {
                ch <- nil
                return
            }
            rows, _ := res.Body["records"].([]any)
            ch <- rows                   // ← send this slave's rows
        }(s)
    }
    buckets := make([][]any, 0, len(alive))
    for range alive {
        if rows := <-ch; rows != nil {
            buckets = append(buckets, rows)  // ← collect each slave's bucket
        }
    }
    close(ch)
    return buckets  // then sent to MapReducer
}
```

---

### 4.3 `writeToTargets()` — `gateway/handlers/helpers.go:58`
**Used for**: UPDATE and DELETE — fan-out writes, sum up affected row counts.

```go
func writeToTargets(targets []*metadata.Slave, method, endpoint string, payload any, countKey string) int {
    ch := make(chan int, len(targets))   // ← channel of integers (affected rows)
    for _, t := range targets {
        go func(sl *metadata.Slave) {    // ← GOROUTINE per target slave
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
        total += <-ch   // ← sum all counts
    }
    close(ch)
    return total
}
```

---

### 4.4 `replicateToSlaves()` — `gateway/metadata/metadata.go:339`
**Used for**: Pushing metadata snapshot to all slaves after any schema change.

```go
func replicateToSlaves() {
    data, _ := marshalSnapshot()
    for _, s := range allSlaves() {
        go func(sl *Slave) {           // ← GOROUTINE per slave (fire-and-forget with retry)
            for attempt := 1; attempt <= 3; attempt++ {
                resp, err := replicationClient.Post(sl.URL+"/shard/metadata/sync", ...)
                if err == nil && resp.StatusCode < 400 {
                    return             // ← success, stop retrying
                }
                time.Sleep(500 * time.Millisecond)
            }
            sl.SetAlive(false)         // ← mark offline after 3 failed attempts
        }(s)
    }
}
```
This is "fire and forget" — no channel needed, just runs independently.

---

### 4.5 `StartHealthChecker()` — `gateway/metadata/metadata.go:376`
**Used for**: Continuously monitoring slave health every 10 seconds.

```go
func StartHealthChecker(interval time.Duration) {
    go func() {                        // ← long-running GOROUTINE (daemon)
        ticker := time.NewTicker(interval)
        for range ticker.C {           // ← fires every 10 seconds
            for _, s := range allSlaves() {
                go func(sl *Slave) {   // ← GOROUTINE per slave (parallel ping)
                    resp, err := cl.Get(sl.URL + "/health")
                    if err != nil {
                        sl.SetAlive(false)
                        return
                    }
                    if !wasAlive {
                        // slave just came back — push snapshot to it immediately
                        go func() { replicationClient.Post(sl.URL+"/shard/metadata/sync", ...) }()
                    }
                    sl.SetAlive(true)
                }(s)
            }
        }
    }()
}
```

---

### 4.6 `watchGateway()` — `slave-go/main.go:303`
**Used for**: slave-go detecting when the gateway goes down.

```go
go watchGateway()   // ← started in slave-go main()

func watchGateway() {
    for {
        time.Sleep(3 * time.Second)
        if atomic.LoadInt32(&isGateway) == 1 { return }  // already promoted
        resp, err := cl.Get(gatewayURL + "/health")
        if err == nil && resp.StatusCode == 200 {
            missed = 0
            continue
        }
        missed++
        if missed >= 3 {
            promote()   // ← take over as gateway
            return
        }
    }
}
```
Uses `atomic.LoadInt32` — safe concurrent read of the `isGateway` flag.

---

### 4.7 `promoHealthChecker()` — `slave-go/main.go:586`
**Used for**: When slave-go is acting as gateway, it monitors other slaves.

```go
func promoHealthChecker(interval time.Duration) {
    ticker := time.NewTicker(interval)
    for range ticker.C {
        for _, s := range slaves {
            go func(sl *promoSlave) {  // ← GOROUTINE per slave (parallel ping)
                resp, err := cl.Get(sl.URL + "/health")
                ...
                sl.SetAlive(true/false)
            }(s)
        }
    }
}
```

---

### 4.8 `demotionChecker()` — `slave-go/main.go:385`
**Used for**: When slave-go is the promoted gateway, it watches for the real gateway to come back.

```go
func demotionChecker(interval time.Duration) {
    ticker := time.NewTicker(interval)
    for {
        select {
        case <-ticker.C:               // ← timer fires every 10s
            resp, _ := cl.Get(gatewayURL + "/health")
            if resp.StatusCode == 200 {
                demote()               // ← original gateway is back, give up role
                return
            }
        case <-promoDemoteChan:        // ← manual signal channel
            demote()
            return
        }
    }
}
```
Uses a **`select`** on two channels — whichever fires first wins.

---

### 4.9 `broadcastAll()` and `promoFanOut()` — `slave-go/main.go:715,749`
Same pattern as the gateway's BroadcastAll/fanOutRows, but used **inside slave-go when it's acting as the promoted gateway**.

```go
func broadcastAll(method, endpoint string, payload any) []fwdResult {
    ch := make(chan fwdResult, len(alive))
    for _, s := range alive {
        go func(sl *promoSlave) { ch <- fwd(sl, method, endpoint, payload) }(s)
    }
    ...collect from channel...
}

func promoFanOut(alive []*promoSlave, path string) []any {
    ch := make(chan []any, len(alive))
    for _, s := range alive {
        go func(sl *promoSlave) {
            res := fwd(sl, "GET", path, nil)
            rows, _ := res.Body["records"].([]any)
            ch <- rows
        }(s)
    }
    merged := []any{}
    for range alive { merged = append(merged, <-ch...) }
    close(ch)
    return merged
}
```

---

### 4.10 `promoWriteCount()` — `slave-go/main.go:773`
Same as `writeToTargets` but for the promoted gateway context.

---

## 5. How Shard Routing Works (FNV Hash)

When you query by `id`, the system needs to know **which slave** holds that row. It uses the **FNV hash algorithm**:

```go
// metadata.go
func (m *TableMeta) SlaveForID(id string) *Slave {
    h := fnv.New32a()
    h.Write([]byte(id))           // hash the id string
    idx := int(h.Sum32()) % m.ShardCount  // modulo to get slave index
    return slaveByID(m.SlaveIDs[idx])      // return that slave
}
```

**Example**: Table has ShardCount=3, SlaveIDs=["slave-a","slave-b","slave-c"]
- id="7" → hash("7")=??? → ??? % 3 = 1 → slave-b
- id="42" → hash("42")=??? → ??? % 3 = 0 → slave-a

This ensures the same `id` **always maps to the same slave**, so reads are routed to the right place.

---

## 6. Round-Robin Insert (`NextInsertSlave`)

```go
func (m *TableMeta) NextInsertSlave() (*Slave, int, error) {
    rrMu.Lock()
    defer rrMu.Unlock()
    for attempt := 0; attempt < m.ShardCount; attempt++ {
        idx := (rrCounter + attempt) % m.ShardCount
        s := slaveByID(m.SlaveIDs[idx])
        if s != nil && s.IsAlive() {
            rrCounter = (idx + 1) % m.ShardCount  // advance counter
            return s, idx, nil
        }
    }
    return nil, 0, fmt.Errorf("no alive slave available")
}
```
- Inserts go to slaves in order: slave-a → slave-b → slave-c → slave-a → ...
- If a slave is down, it skips and tries the next one
- `rrMu` mutex ensures the counter is thread-safe

---

## 7. Metadata Version Control

Every time the schema changes, `version` is incremented:

```go
var version int64
func BumpVersion() int64    { return atomic.AddInt64(&version, 1) }
func CurrentVersion() int64 { return atomic.LoadInt64(&version) }
```

When a snapshot arrives at a slave or the gateway via `/shard/metadata/sync` or `/gateway/sync`:
- **New version > current** → adopt the snapshot
- **Same version** → only merge DroppedDBs (additive)
- **Older version** → ignore (stale data)

This prevents old/stale metadata from overwriting newer state.

---

## 8. Thread Safety (Mutexes)

| Variable         | Mutex Type       | Protects                      |
|------------------|------------------|-------------------------------|
| `Registry`       | `sync.RWMutex`   | Slave list                    |
| `tables`         | `sync.RWMutex`   | Table metadata map            |
| `droppedDBs`     | `sync.RWMutex`   | Dropped database set          |
| `rrCounter`      | `sync.Mutex`     | Round-robin insert counter    |
| `Slave.alive`    | `sync.RWMutex`   | Per-slave alive flag          |
| `version`        | `sync/atomic`    | Metadata version number       |
| `isGateway`      | `sync/atomic`    | Promotion flag in slave-go    |

`RWMutex` = multiple goroutines can **read** at the same time, but only one can **write**.  
`atomic` = lock-free safe operations for simple integers.

---

## 9. The Three Slave Implementations

All 3 slaves expose the exact same endpoints: `/shard/db/create`, `/shard/table/create`, `/shard/query/insert`, etc. They differ only in implementation language:

| Feature         | slave-go (Go)          | slave-python (Python)    | slave-dotNet (.NET/C#)     |
|-----------------|------------------------|--------------------------|----------------------------|
| Framework       | `net/http` stdlib      | Flask                    | ASP.NET Core Minimal API   |
| Auth            | HMAC verify in Go      | HMAC verify in Python    | HMAC verify in C#          |
| Replica DB      | via `storage` package  | `{db}_replica`           | `{db}_replica`             |
| Extra role      | Can promote to gateway | None                     | None                       |

---

## 10. MapReducer — How it Works

```
Gateway → POST /reduce {
    "shards": [
        [{"id":1,"name":"Ali"},{"id":4,"name":"Sara"}],   // from slave-a
        [{"id":2,"name":"Omar"}],                          // from slave-b
        [{"id":3,"name":"Lina"},{"id":6,"name":"Hana"}]   // from slave-c
    ],
    "order_by": "name",
    "order": "asc",
    "limit": 3
}

MapReducer:
    1. Merge all arrays → [Ali, Sara, Omar, Lina, Hana] (5 rows)
    2. Sort by "name" asc → [Ali, Hana, Lina, Omar, Sara]
    3. Limit to 3 → [Ali, Hana, Lina]

Returns: {"count":3,"records":[Ali,Hana,Lina]}
```

---

## 11. HTTP Endpoints Summary

### Gateway Endpoints (public-facing)
| Method   | Path               | Purpose                        |
|----------|--------------------|--------------------------------|
| POST     | /db/create         | Create a database on all shards|
| DELETE   | /db/drop           | Drop a database on all shards  |
| POST     | /table/create      | Create a sharded table         |
| DELETE   | /table/drop        | Drop a table from all shards   |
| POST     | /query/insert      | Insert a row (round-robin)     |
| GET      | /query/select      | Select rows (fan-out + merge)  |
| PUT      | /query/update      | Update rows (targeted/all)     |
| DELETE   | /query/delete      | Delete rows (targeted/all)     |
| GET      | /query/search      | Full-text search (fan-out)     |
| GET      | /health            | Gateway health check           |
| GET      | /gateway/status    | Show slaves & metadata version |
| POST     | /gateway/promote   | Receive promotion notice       |
| POST     | /gateway/sync      | Receive metadata from promoted slave |

### Slave Endpoints (internal, require X-Gateway-Token)
| Method   | Path                    | Purpose                |
|----------|-------------------------|------------------------|
| POST     | /shard/db/create        | Create DB in MySQL     |
| DELETE   | /shard/db/drop          | Drop DB in MySQL       |
| POST     | /shard/table/create     | Create table in MySQL  |
| DELETE   | /shard/table/drop       | Drop table in MySQL    |
| POST     | /shard/query/insert     | Insert row in MySQL    |
| GET      | /shard/query/select     | Select rows from MySQL |
| PUT      | /shard/query/update     | Update rows in MySQL   |
| DELETE   | /shard/query/delete     | Delete rows in MySQL   |
| GET      | /shard/query/search     | Full-text search       |
| POST     | /shard/metadata/sync    | Receive metadata update|
| GET      | /health                 | Slave health check     |

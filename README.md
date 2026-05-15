# Distributed Database System

A **Federated Distributed Database** built with Go, Python, and .NET. Data is horizontally sharded across multiple slave nodes. A central **Gateway** handles routing, metadata management, health checking, and automatic failover.

---

## Architecture

```
                     ┌──────────────────────────────┐
      CLIENT         │        GATEWAY  (:8080)       │
  (HTTP / curl)  ──► │  Routing · Metadata · Auth    │
                     └─────────────┬────────────────┘
                                   │  broadcasts / forwards
                   ┌───────────────┼───────────────┐
                   ▼               ▼               ▼
            slave-a (:8081)  slave-b (:8082)  slave-c (:8083)
             [Go + MySQL]   [Python + MySQL]  [.NET + MySQL]
                   │               │               │
            [MapReducer (:8090)] ◄─┘───────────────┘
              Merges · Sorts · Limits results
```

| Service    | Port | Language    |
|------------|------|-------------|
| Gateway    | 8080 | Go          |
| slave-a    | 8081 | Go          |
| slave-b    | 8082 | Python      |
| slave-c    | 8083 | .NET / C#   |
| MapReducer | 8090 | Go          |

---

## Prerequisites

| Requirement | Version  | Notes |
|-------------|----------|-------|
| Go          | ≥ 1.21   | For gateway, slave-go, mapreducer |
| Python      | ≥ 3.10   | For slave-b |
| .NET SDK    | ≥ 8.0    | For slave-c |
| MySQL       | ≥ 8.0    | All slaves connect to MySQL |

> All slaves share the **same MySQL instance** by default (different databases per shard).  
> The shared HMAC secret is `Hana-1234` — change via the `SLAVE_SHARED_SECRET` env variable.

---

## Setup & Running

### 1. Clone the repository

```bash
git clone https://github.com/hanasoliman72/distributed-db.git
cd distributed-db
```

### 2. Start MySQL

Ensure MySQL is running on `127.0.0.1:3306` with user `root` / password `root`.  
You can override this with environment variables (see [Configuration](#configuration)).

---

### 3. Start the MapReducer

```bash
cd mapreducer
go run main.go
# Listening on :8090
```

---

### 4. Start slave-a (Go)

```bash
cd slave-go
go run main.go
# Connected to MySQL
# Listening on :8081
# Watchdog: pinging gateway every 3s
```

---

### 5. Start slave-b (Python)

```bash
cd slave-python
pip install -r requirements.txt
python app.py
# Python slave listening on :8082
```

---

### 6. Start slave-c (.NET)

```bash
cd slave-dotNet
dotnet run
# .NET slave listening on :8083
```

---

### 7. Start the Gateway (last)

```bash
cd gateway
go run .
# HMAC secret loaded
# Slaves registered: :8081  :8082  :8083
# Health checker started (interval=10s)
# Listening on :8080
```

> **Order matters**: start the slaves **before** the gateway so the initial health check marks them all alive and the startup replication succeeds.

---

## Configuration

All services are configurable via environment variables:

### Gateway (hardcoded constants in `main.go`)

| Constant        | Default                  | Description              |
|-----------------|--------------------------|--------------------------|
| `defaultPort`   | `:8080`                  | Gateway listen port      |
| `gatewaySecret` | `Hana-1234`              | HMAC shared secret       |
| `slaveAURL`     | `http://127.0.0.1:8081`  | slave-a address          |
| `slaveBURL`     | `http://127.0.0.1:8082`  | slave-b address          |
| `slaveCURL`     | `http://127.0.0.1:8083`  | slave-c address          |

### Slaves (environment variables)

| Variable              | Default       | Description              |
|-----------------------|---------------|--------------------------|
| `SLAVE_SHARED_SECRET` | `Hana-1234`   | Must match gateway secret |
| `MYSQL_HOST`          | `127.0.0.1`   | MySQL host               |
| `MYSQL_PORT`          | `3306`        | MySQL port               |
| `MYSQL_USER`          | `root`        | MySQL user               |
| `MYSQL_PASSWORD`      | `root`        | MySQL password           |
| `SLAVE_PORT`          | `:8081`       | slave-go listen port     |
| `SLAVE_ID`            | `slave-a`     | slave-go node identifier |

---

## Usage Examples

All requests go to the **Gateway** (`http://localhost:8080`).

---

### Health Check

```bash
curl http://localhost:8080/health
# {"role":"gateway","status":"ok"}

curl http://localhost:8080/gateway/status
# {"dropped_dbs":[],"slaves":[...],"version":5}
```

---

### DDL — Databases

**Create a database** (broadcasts to all shards):
```bash
curl -X POST http://localhost:8080/db/create \
  -H "Content-Type: application/json" \
  -d '{"db": "university"}'
# {"message":"database 'university' created on all shards"}
```

**Drop a database**:
```bash
curl -X DELETE http://localhost:8080/db/drop \
  -H "Content-Type: application/json" \
  -d '{"db": "university"}'
# {"message":"database 'university' dropped; shard routing retained for replica fallback"}
```

---

### DDL — Tables

**Create a table** (sharded across all alive slaves):
```bash
curl -X POST http://localhost:8080/table/create \
  -H "Content-Type: application/json" \
  -d '{
    "db": "university",
    "table": "students",
    "attributes": ["name", "email", "major"]
  }'
# {"message":"table 'students' created","shard_count":3,"shards":["slave-a","slave-b","slave-c"]}
```

**Drop a table**:
```bash
curl -X DELETE http://localhost:8080/table/drop \
  -H "Content-Type: application/json" \
  -d '{"db": "university", "table": "students"}'
# {"message":"table 'students' dropped"}
```

---

### DML — Insert

Rows are distributed using **round-robin** across shards:

```bash
curl -X POST http://localhost:8080/query/insert \
  -H "Content-Type: application/json" \
  -d '{
    "db": "university",
    "table": "students",
    "record": {"name": "Ali Hassan", "email": "ali@uni.edu", "major": "CS"}
  }'
# {"generated_id":1,"message":"record inserted","shard":"slave-a"}

curl -X POST http://localhost:8080/query/insert \
  -H "Content-Type: application/json" \
  -d '{
    "db": "university",
    "table": "students",
    "record": {"name": "Sara Ahmed", "email": "sara@uni.edu", "major": "EE"}
  }'
# {"generated_id":1,"message":"record inserted","shard":"slave-b"}
```

---

### DML — Select

**Select all rows** (fan-out to all shards, merged by MapReducer):
```bash
curl "http://localhost:8080/query/select?db=university&table=students"
# {"count":2,"records":[{"email":"ali@uni.edu","id":1,"major":"CS","name":"Ali Hassan"},{"email":"sara@uni.edu","id":1,"major":"EE","name":"Sara Ahmed"}]}
```

**Select with filter** (broadcasts where clause to all shards):
```bash
curl "http://localhost:8080/query/select?db=university&table=students&major=CS"
```

**Select by id** (routes to the exact shard using FNV hash — fast path):
```bash
curl "http://localhost:8080/query/select?db=university&table=students&id=1"
```

**Select with sorting and limit**:
```bash
curl "http://localhost:8080/query/select?db=university&table=students&order_by=name&order=asc&limit=5"
```

---

### DML — Update

**Update by id** (routes to the single owning shard):
```bash
curl -X PUT http://localhost:8080/query/update \
  -H "Content-Type: application/json" \
  -d '{
    "db": "university",
    "table": "students",
    "where": {"id": 1},
    "set": {"major": "Math"}
  }'
# {"message":"update complete","records_updated":1}
```

**Update without id** (broadcasts to all shards):
```bash
curl -X PUT http://localhost:8080/query/update \
  -H "Content-Type: application/json" \
  -d '{
    "db": "university",
    "table": "students",
    "where": {"major": "CS"},
    "set": {"major": "Computer Science"}
  }'
```

---

### DML — Delete

**Delete by id** (routes to single shard):
```bash
curl -X DELETE http://localhost:8080/query/delete \
  -H "Content-Type: application/json" \
  -d '{
    "db": "university",
    "table": "students",
    "where": {"id": 1}
  }'
# {"message":"delete complete","records_deleted":1}
```

---

### DML — Full-Text Search

Searches all string fields across all shards:
```bash
curl "http://localhost:8080/query/search?db=university&table=students&q=ali"
# {"count":1,"records":[{"email":"ali@uni.edu","id":1,...}],"search_term":"ali"}
```

---

## Failover & Recovery

The system includes **automatic leader election** (slave-go only):

1. **Gateway goes down** → slave-go detects 3 consecutive missed pings → promotes itself to act as gateway on `:8080`
2. **Original gateway comes back** → slave-go detects it → syncs its metadata to the gateway → demotes itself back to slave-only mode
3. **Slave goes down** → gateway health checker marks it offline within 10 seconds → all requests skip the offline slave
4. **Slave comes back** → gateway detects it and immediately pushes the current metadata snapshot to it

---

## Project Structure

```
distributed-db/
├── gateway/                  # Gateway service (Go)
│   ├── main.go               # Startup, HTTP mux, promote/sync endpoints
│   ├── auth/
│   │   └── auth.go           # HMAC token generation
│   ├── handlers/
│   │   ├── ddl.go            # CREATE/DROP DB and TABLE handlers
│   │   ├── dml.go            # INSERT/SELECT/UPDATE/DELETE/SEARCH handlers
│   │   └── helpers.go        # broadcast(), fanOutRows(), writeToTargets()
│   ├── metadata/
│   │   └── metadata.go       # Slave registry, TableMeta, Snapshot, health checker
│   ├── shard/
│   │   └── shared.go         # Forward(), ForwardGet(), BroadcastAll()
│   └── metadata.json         # Persisted cluster state
│
├── slave-go/                 # Slave node A (Go) — also handles failover promotion
│   ├── main.go               # Slave HTTP server + watchGateway + promote/demote logic
│   └── storage/              # MySQL abstraction layer
│
├── slave-python/             # Slave node B (Python / Flask)
│   ├── app.py
│   └── requirements.txt
│
├── slave-dotNet/             # Slave node C (.NET / C# Minimal API)
│   ├── Program.cs
│   └── Slavenode.csproj
│
└── mapreducer/               # Merge service (Go)
    └── main.go               # /reduce endpoint — merge, sort, limit
```

---

## API Reference

### Gateway Endpoints

| Method   | Endpoint            | Body / Query Params                              |
|----------|---------------------|--------------------------------------------------|
| `GET`    | `/health`           | —                                                |
| `GET`    | `/gateway/status`   | —                                                |
| `POST`   | `/db/create`        | `{"db":"name"}`                                  |
| `DELETE` | `/db/drop`          | `{"db":"name"}`                                  |
| `POST`   | `/table/create`     | `{"db","table","attributes":["col1",...]}`       |
| `DELETE` | `/table/drop`       | `{"db","table"}`                                 |
| `POST`   | `/query/insert`     | `{"db","table","record":{"col":"val"}}`          |
| `GET`    | `/query/select`     | `?db=&table=[&col=val][&order_by=&order=&limit=]`|
| `PUT`    | `/query/update`     | `{"db","table","where":{},"set":{}}`             |
| `DELETE` | `/query/delete`     | `{"db","table","where":{}}`                      |
| `GET`    | `/query/search`     | `?db=&table=&q=searchterm`                       |

### Internal Slave Endpoints (require `X-Gateway-Token` header)

| Method   | Endpoint                 |
|----------|--------------------------|
| `POST`   | `/shard/db/create`       |
| `DELETE` | `/shard/db/drop`         |
| `POST`   | `/shard/table/create`    |
| `DELETE` | `/shard/table/drop`      |
| `POST`   | `/shard/query/insert`    |
| `GET`    | `/shard/query/select`    |
| `PUT`    | `/shard/query/update`    |
| `DELETE` | `/shard/query/delete`    |
| `GET`    | `/shard/query/search`    |
| `POST`   | `/shard/metadata/sync`   |
| `GET`    | `/health`                |

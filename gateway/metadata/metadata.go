package metadata

// metadata.go
//
// The API Gateway holds NO row data.  Instead it maintains:
//   1. SlaveRegistry – the list of slave nodes and their liveness.
//   2. ShardMap      – for each (db, table) the hash-range → slave assignment.
//
// Sharding strategy: horizontal hash partitioning by primary key modulo N.
//   shard_index = hash(id) % len(alive_slaves)
//
// For INSERT (no id yet) the gateway uses round-robin across alive slaves and
// stores the assignment so future updates / deletes can route correctly.
//
// Metadata is kept in memory only (no persistence for the prototype).  Add a
// BoltDB / SQLite backend if you need durability across gateway restarts.

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"sync"
	"time"
)

// ── Slave registry ─────────────────────────────────────────────────────────

// Slave represents one data node.
type Slave struct {
	ID  string // e.g. "slave-a"
	URL string // e.g. "http://127.0.0.1:8081"

	mu    sync.RWMutex
	alive bool
}

func newSlave(id, url string) *Slave { return &Slave{ID: id, URL: url, alive: true} }

func (s *Slave) IsAlive() bool   { s.mu.RLock(); defer s.mu.RUnlock(); return s.alive }
func (s *Slave) SetAlive(v bool) { s.mu.Lock(); defer s.mu.Unlock(); s.alive = v }

// Registry holds all known slaves.
var Registry []*Slave

// RegisterSlave adds a slave to the global registry.
func RegisterSlave(id, url string) {
	Registry = append(Registry, newSlave(id, url))
}

// AliveSalves returns currently healthy slaves in order.
func AliveSlaves() []*Slave {
	out := make([]*Slave, 0, len(Registry))
	for _, s := range Registry {
		if s.IsAlive() {
			out = append(out, s)
		}
	}
	return out
}

// ── Shard map ──────────────────────────────────────────────────────────────

// TableMeta holds the shard assignments for one (db, table) pair.
type TableMeta struct {
	DB         string
	Table      string
	Attributes []string

	// ShardCount is the number of shards when the table was created.
	// Changing this requires a re-shard (not implemented in prototype).
	ShardCount int

	// SlaveIDs[i] is the slave that owns shard i (0-based).
	SlaveIDs []string
}

var (
	tableMu sync.RWMutex
	tables  = map[string]*TableMeta{} // key: "db.table"
)

func tableKey(db, table string) string { return db + "." + table }

// CreateTableMeta records a new table using the current set of alive slaves.
func CreateTableMeta(db, table string, attributes []string) *TableMeta {
	alive := AliveSlaves()
	ids := make([]string, len(alive))
	for i, s := range alive {
		ids[i] = s.ID
	}
	m := &TableMeta{
		DB:         db,
		Table:      table,
		Attributes: attributes,
		ShardCount: len(alive),
		SlaveIDs:   ids,
	}
	tableMu.Lock()
	tables[tableKey(db, table)] = m
	tableMu.Unlock()
	return m
}

// GetTableMeta returns the metadata for (db, table), or nil if unknown.
func GetTableMeta(db, table string) *TableMeta {
	tableMu.RLock()
	defer tableMu.RUnlock()
	return tables[tableKey(db, table)]
}

// DropTableMeta removes the metadata entry.
func DropTableMeta(db, table string) {
	tableMu.Lock()
	delete(tables, tableKey(db, table))
	tableMu.Unlock()
}

// TablesInDB returns metadata for every table in a database.
func TablesInDB(db string) []*TableMeta {
	tableMu.RLock()
	defer tableMu.RUnlock()
	var out []*TableMeta
	for _, m := range tables {
		if m.DB == db {
			out = append(out, m)
		}
	}
	return out
}

// ── Routing helpers ────────────────────────────────────────────────────────

// SlaveForID returns the slave that owns the given row id within a table.
// Uses consistent hash: shard = fnv32(id) % ShardCount.
func (m *TableMeta) SlaveForID(id string) *Slave {
	h := fnv.New32a()
	h.Write([]byte(id))
	idx := int(h.Sum32()) % m.ShardCount
	slaveID := m.SlaveIDs[idx]
	for _, s := range Registry {
		if s.ID == slaveID {
			return s
		}
	}
	return nil
}

// NextInsertSlave picks a slave for a new INSERT using round-robin over alive
// slaves, constrained to the shards assigned to this table.
var rrMu sync.Mutex
var rrCounter int

func (m *TableMeta) NextInsertSlave() (*Slave, int, error) {
	rrMu.Lock()
	defer rrMu.Unlock()

	// try each shard slot in round-robin order
	for attempt := 0; attempt < m.ShardCount; attempt++ {
		idx := (rrCounter + attempt) % m.ShardCount
		slaveID := m.SlaveIDs[idx]
		for _, s := range Registry {
			if s.ID == slaveID && s.IsAlive() {
				rrCounter = (idx + 1) % m.ShardCount
				return s, idx, nil
			}
		}
	}
	return nil, 0, fmt.Errorf("metadata: no alive slave available for table %s.%s", m.DB, m.Table)
}

// ── Health checker ─────────────────────────────────────────────────────────

// StartHealthChecker pings every slave every interval seconds.
func StartHealthChecker(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			for _, s := range Registry {
				go func(sl *Slave) {
					cl := &http.Client{Timeout: 3 * time.Second}
					resp, err := cl.Get(sl.URL + "/health")
					if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
						if resp != nil {
							resp.Body.Close()
						}
						if sl.IsAlive() {
							log.Printf("[metadata] slave %s went OFFLINE", sl.ID)
						}
						sl.SetAlive(false)
					} else {
						if !sl.IsAlive() {
							log.Printf("[metadata] slave %s back ONLINE", sl.ID)
						}
						sl.SetAlive(true)
						resp.Body.Close()
					}
				}(s)
			}
		}
	}()
}

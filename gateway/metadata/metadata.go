package metadata

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const MetadataFile = "metadata.json"

// ── Slave registry ─────────────────────────────────────────────────────────
type Slave struct {
	ID  string // e.g. "slave-a"
	URL string // e.g. "http://127.0.0.1:8081"

	mu    sync.RWMutex
	alive bool
}

func newSlave(id, url string) *Slave { return &Slave{ID: id, URL: url, alive: true} }

func (s *Slave) IsAlive() bool   { s.mu.RLock(); defer s.mu.RUnlock(); return s.alive }
func (s *Slave) SetAlive(v bool) { s.mu.Lock(); defer s.mu.Unlock(); s.alive = v }

var Registry []*Slave

func RegisterSlave(id, url string) {
	Registry = append(Registry, newSlave(id, url))
}

// AliveSlaves returns currently healthy slaves in order.
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
	ShardCount int
	SlaveIDs   []string
}

var (
	tableMu sync.RWMutex
	tables  = map[string]*TableMeta{}
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

// ── Persistence ────────────────────────────────────────────────────────────

// persistedMetadata is the JSON structure written to MetadataFile.
type persistedMetadata struct {
	Slaves []persistedSlave `json:"slaves"`
	Tables []*TableMeta     `json:"tables"`
}

type persistedSlave struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// SaveMetadata writes the current registry and shard map to MetadataFile.
// Called after every DDL change so slave-go always has an up-to-date snapshot.
func SaveMetadata() {
	slaves := make([]persistedSlave, len(Registry))
	for i, s := range Registry {
		slaves[i] = persistedSlave{ID: s.ID, URL: s.URL}
	}

	tableMu.RLock()
	tlist := make([]*TableMeta, 0, len(tables))
	for _, m := range tables {
		tlist = append(tlist, m)
	}
	tableMu.RUnlock()

	data, err := json.MarshalIndent(persistedMetadata{Slaves: slaves, Tables: tlist}, "", "  ")
	if err != nil {
		log.Printf("[metadata] SaveMetadata: marshal error: %v", err)
		return
	}
	if err := os.WriteFile(MetadataFile, data, 0644); err != nil {
		log.Printf("[metadata] SaveMetadata: write error: %v", err)
		return
	}
	log.Printf("[metadata] saved to %s (%d tables, %d slaves)", MetadataFile, len(tlist), len(slaves))
}

// LoadMetadata reads MetadataFile and restores the registry and shard map.
// Called at startup (both by the normal gateway and by slave-go when it
func LoadMetadata() error {
	data, err := os.ReadFile(MetadataFile)
	if err != nil {
		return fmt.Errorf("LoadMetadata: read: %w", err)
	}
	return LoadMetadataFromBytes(data)
}

// LoadMetadataFromBytes validates and loads persisted metadata directly from JSON.
func LoadMetadataFromBytes(data []byte) error {
	var pm persistedMetadata
	if err := json.Unmarshal(data, &pm); err != nil {
		return fmt.Errorf("LoadMetadataFromBytes: unmarshal: %w", err)
	}

	// Fully replace the in-memory registry and shard map to match persisted state.
	Registry = nil
	tableMu.Lock()
	tables = make(map[string]*TableMeta)
	for _, m := range pm.Tables {
		tables[tableKey(m.DB, m.Table)] = m
	}
	tableMu.Unlock()

	for _, s := range pm.Slaves {
		RegisterSlave(s.ID, s.URL)
	}

	log.Printf("[metadata] loaded from %s (%d tables, %d slaves)", MetadataFile, len(pm.Tables), len(pm.Slaves))
	return nil
}

// ── Routing helpers ────────────────────────────────────────────────────────

// SlaveForID returns the slave that owns the given row id within a table.
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

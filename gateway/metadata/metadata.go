package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const MetadataFile = "metadata.json"

var version int64

func CurrentVersion() int64 { return atomic.LoadInt64(&version) }
func BumpVersion() int64    { return atomic.AddInt64(&version, 1) }
func SetVersion(v int64)    { atomic.StoreInt64(&version, v) }

var (
	droppedMu  sync.RWMutex
	droppedDBs = map[string]struct{}{}
)

func IsDBDropped(db string) bool {
	droppedMu.RLock()
	defer droppedMu.RUnlock()
	_, ok := droppedDBs[db]
	return ok
}
func markDropped(db string) {
	droppedMu.Lock()
	droppedDBs[db] = struct{}{}
	droppedMu.Unlock()
}
func ClearDroppedDB(db string) {
	droppedMu.Lock()
	delete(droppedDBs, db)
	droppedMu.Unlock()
}
func setDroppedDBs(dbs []string) {
	droppedMu.Lock()
	droppedDBs = make(map[string]struct{}, len(dbs))
	for _, d := range dbs {
		droppedDBs[d] = struct{}{}
	}
	droppedMu.Unlock()
}
func droppedDBList() []string {
	droppedMu.RLock()
	defer droppedMu.RUnlock()
	out := make([]string, 0, len(droppedDBs))
	for d := range droppedDBs {
		out = append(out, d)
	}
	return out
}

type Slave struct {
	ID  string
	URL string

	mu    sync.RWMutex
	alive bool
}

func newSlave(id, url string) *Slave { return &Slave{ID: id, URL: url, alive: true} }

func (s *Slave) IsAlive() bool   { s.mu.RLock(); defer s.mu.RUnlock(); return s.alive }
func (s *Slave) SetAlive(v bool) { s.mu.Lock(); defer s.mu.Unlock(); s.alive = v }

var registryMu sync.RWMutex
var Registry []*Slave

func RegisterSlave(id, url string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, s := range Registry {
		if s.ID == id {
			s.URL = url
			return
		}
	}
	Registry = append(Registry, newSlave(id, url))
}
func AliveSlaves() []*Slave {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]*Slave, 0, len(Registry))
	for _, s := range Registry {
		if s.IsAlive() {
			out = append(out, s)
		}
	}
	return out
}
func allSlaves() []*Slave {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]*Slave, len(Registry))
	copy(out, Registry)
	return out
}
func slaveByID(id string) *Slave {
	registryMu.RLock()
	defer registryMu.RUnlock()
	for _, s := range Registry {
		if s.ID == id {
			return s
		}
	}
	return nil
}

// ── Shard / table map ──────────────────────────────────────────────────────
type TableMeta struct {
	DB         string   `json:"DB"`
	Table      string   `json:"Table"`
	Attributes []string `json:"Attributes"`
	ShardCount int      `json:"ShardCount"`
	SlaveIDs   []string `json:"SlaveIDs"`
}

var (
	tableMu sync.RWMutex
	tables  = map[string]*TableMeta{}
)

func tableKey(db, table string) string { return db + "." + table }
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

	BumpVersion()
	SaveMetadata()
	go replicateToSlaves()
	return m
}

func GetTableMeta(db, table string) *TableMeta {
	tableMu.RLock()
	defer tableMu.RUnlock()
	return tables[tableKey(db, table)]
}
func DropTableMeta(db, table string) {
	tableMu.Lock()
	delete(tables, tableKey(db, table))
	tableMu.Unlock()

	BumpVersion()
	SaveMetadata()
	go replicateToSlaves()
}
func MarkDBDropped(db string) {
	markDropped(db)
	BumpVersion()
	SaveMetadata()
	go replicateToSlaves()
}

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

type PersistedSlave struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type Snapshot struct {
	Version    int64            `json:"version"`
	Slaves     []PersistedSlave `json:"slaves"`
	Tables     []*TableMeta     `json:"tables"`
	DroppedDBs []string         `json:"dropped_dbs,omitempty"`
}

func marshalSnapshot() ([]byte, error) {
	registryMu.RLock()
	slaves := make([]PersistedSlave, len(Registry))
	for i, s := range Registry {
		slaves[i] = PersistedSlave{ID: s.ID, URL: s.URL}
	}
	registryMu.RUnlock()
	tableMu.RLock()
	tlist := make([]*TableMeta, 0, len(tables))
	for _, m := range tables {
		tlist = append(tlist, m)
	}
	tableMu.RUnlock()
	snap := Snapshot{
		Version:    CurrentVersion(),
		Slaves:     slaves,
		Tables:     tlist,
		DroppedDBs: droppedDBList(),
	}
	return json.MarshalIndent(snap, "", "  ")
}

func SaveMetadata() {
	data, err := marshalSnapshot()
	if err != nil {
		log.Printf("[metadata] SaveMetadata: marshal error: %v", err)
		return
	}
	tmp := MetadataFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("[metadata] SaveMetadata: write tmp error: %v", err)
		return
	}
	if err := os.Rename(tmp, MetadataFile); err != nil {
		log.Printf("[metadata] SaveMetadata: rename error: %v", err)
		return
	}
	tableMu.RLock()
	n := len(tables)
	tableMu.RUnlock()
	registryMu.RLock()
	r := len(Registry)
	registryMu.RUnlock()
	log.Printf("[metadata] saved (version=%d, tables=%d, slaves=%d, dropped_dbs=%d)",
		CurrentVersion(), n, r, len(droppedDBList()))
}

func LoadMetadata() error {
	data, err := os.ReadFile(MetadataFile)
	if err != nil {
		return fmt.Errorf("LoadMetadata: read: %w", err)
	}
	return LoadMetadataFromBytes(data)
}

func LoadMetadataFromBytes(data []byte) error {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("LoadMetadataFromBytes: unmarshal: %w", err)
	}

	cur := CurrentVersion()

	if snap.Version > 0 && snap.Version < cur {
		log.Printf("[metadata] ignoring stale snapshot (incoming=%d current=%d)", snap.Version, cur)
		return nil
	}

	if snap.Version > 0 && snap.Version == cur {
		if len(snap.DroppedDBs) > 0 {
			droppedMu.Lock()
			for _, d := range snap.DroppedDBs {
				droppedDBs[d] = struct{}{}
			}
			droppedMu.Unlock()
			log.Printf("[metadata] merged DroppedDBs from peer (version=%d)", cur)
		}
		return nil
	}

	tableMu.Lock()
	tables = make(map[string]*TableMeta, len(snap.Tables))
	for _, m := range snap.Tables {
		tables[tableKey(m.DB, m.Table)] = m
	}
	tableMu.Unlock()

	registryMu.Lock()
	existing := make(map[string]*Slave, len(Registry))
	for _, s := range Registry {
		existing[s.ID] = s
	}
	newRegistry := make([]*Slave, 0, len(snap.Slaves))
	for _, ps := range snap.Slaves {
		if old, ok := existing[ps.ID]; ok {
			old.URL = ps.URL
			newRegistry = append(newRegistry, old)
		} else {
			newRegistry = append(newRegistry, newSlave(ps.ID, ps.URL))
		}
	}
	Registry = newRegistry
	registryMu.Unlock()

	setDroppedDBs(snap.DroppedDBs)
	if snap.Version > 0 {
		SetVersion(snap.Version)
	}
	log.Printf("[metadata] loaded (version=%d, tables=%d, slaves=%d, dropped_dbs=%v)",
		snap.Version, len(snap.Tables), len(snap.Slaves), snap.DroppedDBs)
	return nil
}

func CurrentSnapshot() Snapshot {
	registryMu.RLock()
	slaves := make([]PersistedSlave, len(Registry))
	for i, s := range Registry {
		slaves[i] = PersistedSlave{ID: s.ID, URL: s.URL}
	}
	registryMu.RUnlock()
	tableMu.RLock()
	tlist := make([]*TableMeta, 0, len(tables))
	for _, m := range tables {
		tlist = append(tlist, m)
	}
	tableMu.RUnlock()
	return Snapshot{
		Version:    CurrentVersion(),
		Slaves:     slaves,
		Tables:     tlist,
		DroppedDBs: droppedDBList(),
	}
}

var replicationClient = &http.Client{Timeout: 5 * time.Second}

func replicateToSlaves() {
	data, err := marshalSnapshot()
	if err != nil {
		log.Printf("[metadata] replicateToSlaves: marshal: %v", err)
		return
	}
	for _, s := range allSlaves() {
		go func(sl *Slave) {
			const maxAttempts = 3
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				resp, err := replicationClient.Post(
					sl.URL+"/shard/metadata/sync",
					"application/json",
					bytes.NewReader(data),
				)
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode < 400 {
						log.Printf("[metadata] replicate→%s OK (version=%d, attempt=%d)",
							sl.ID, CurrentVersion(), attempt)
						return
					}
				}
				log.Printf("[metadata] replicate→%s attempt %d/%d failed: %v",
					sl.ID, attempt, maxAttempts, err)
				if attempt < maxAttempts {
					time.Sleep(500 * time.Millisecond)
				}
			}
			sl.SetAlive(false)
			log.Printf("[metadata] replicate→%s: all attempts failed; marked OFFLINE", sl.ID)
		}(s)
	}
}
func ReplicateToSlaves() {
	go replicateToSlaves()
}
func StartHealthChecker(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			for _, s := range allSlaves() {
				go func(sl *Slave) {
					cl := &http.Client{Timeout: 3 * time.Second}
					resp, err := cl.Get(sl.URL + "/health")
					wasAlive := sl.IsAlive()

					if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
						if resp != nil {
							resp.Body.Close()
						}
						if wasAlive {
							log.Printf("[metadata] slave %s went OFFLINE", sl.ID)
						}
						sl.SetAlive(false)
						return
					}
					resp.Body.Close()

					if !wasAlive {
						log.Printf("[metadata] slave %s back ONLINE — pushing snapshot (version=%d)",
							sl.ID, CurrentVersion())
						go func() {
							data, err := marshalSnapshot()
							if err != nil {
								return
							}
							r, err := replicationClient.Post(
								sl.URL+"/shard/metadata/sync",
								"application/json",
								bytes.NewReader(data),
							)
							if err == nil {
								r.Body.Close()
							}
						}()
					}
					sl.SetAlive(true)
				}(s)
			}
		}
	}()
}
func (m *TableMeta) SlaveForID(id string) *Slave {
	h := fnv.New32a()
	h.Write([]byte(id))
	idx := int(h.Sum32()) % m.ShardCount
	return slaveByID(m.SlaveIDs[idx])
}

var rrMu sync.Mutex
var rrCounter int

func (m *TableMeta) NextInsertSlave() (*Slave, int, error) {
	rrMu.Lock()
	defer rrMu.Unlock()
	for attempt := 0; attempt < m.ShardCount; attempt++ {
		idx := (rrCounter + attempt) % m.ShardCount
		s := slaveByID(m.SlaveIDs[idx])
		if s != nil && s.IsAlive() {
			rrCounter = (idx + 1) % m.ShardCount
			return s, idx, nil
		}
	}
	return nil, 0, fmt.Errorf("metadata: no alive slave available for table %s.%s", m.DB, m.Table)
}

package metadata

// metadata.go
//
// Shard-map, slave registry, persistence, and active metadata replication.
//
// REPLICATION DESIGN
// ──────────────────
// Every DDL mutation bumps a monotonic Version counter then calls
// replicateToSlaves() which POSTs the full Snapshot to every slave's
// /shard/metadata/sync endpoint. Slaves persist it locally so they can
// bootstrap as promoted gateways even after a restart.
//
// When the original gateway comes back the promoted slave POSTs its Snapshot
// to /gateway/sync. LoadMetadataFromBytes() applies a version guard: only
// adopt state that is strictly newer than what is already in memory.
//
// DB-DROP RESILIENCE
// ──────────────────
// Dropping a database does NOT remove its tables from the shard map.
// Instead the DB name is added to Snapshot.DroppedDBs.
// The slave storage layer already falls back to <db>_replica transparently
// AND can use DroppedDBs to skip the primary attempt entirely, avoiding
// unnecessary round-trip timeouts.
// By keeping the TableMeta entries alive the gateway still routes queries
// to the right shards; reads/writes silently use the replica schema.

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

// ── Version counter ────────────────────────────────────────────────────────

var version int64 // monotonic; bumped on every DDL change

func CurrentVersion() int64        { return atomic.LoadInt64(&version) }
func BumpVersion() int64           { return atomic.AddInt64(&version, 1) }
func SetVersion(v int64)           { atomic.StoreInt64(&version, v) }

// ── Dropped-DB registry ────────────────────────────────────────────────────

var (
	droppedMu  sync.RWMutex
	droppedDBs = map[string]struct{}{} // set of physically-dropped DB names
)

// IsDBDropped returns true if the given database has been dropped via /db/drop.
// Slave storage uses this to skip the primary-DB attempt and go straight to replica.
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

// ClearDroppedDB removes db from the dropped set.
// Called by /db/create so that re-creating a database immediately restores
// primary-first routing — on the gateway AND on every slave (via replication).
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

// ── Slave registry ─────────────────────────────────────────────────────────

type Slave struct {
	ID  string
	URL string

	mu    sync.RWMutex
	alive bool
}

func newSlave(id, url string) *Slave { return &Slave{ID: id, URL: url, alive: true} }

func (s *Slave) IsAlive() bool   { s.mu.RLock(); defer s.mu.RUnlock(); return s.alive }
func (s *Slave) SetAlive(v bool) { s.mu.Lock(); defer s.mu.Unlock(); s.alive = v }

// registryMu guards Registry so that concurrent LoadMetadataFromBytes calls
// and health-checker iterations never race on the slice.
var registryMu sync.RWMutex
var Registry []*Slave

// RegisterSlave appends a new slave.  Must be called under registryMu.Lock
// or at startup before any goroutines start (main() is safe).
func RegisterSlave(id, url string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	// Avoid duplicate registrations.
	for _, s := range Registry {
		if s.ID == id {
			s.URL = url // update URL in case it changed
			return
		}
	}
	Registry = append(Registry, newSlave(id, url))
}

// AliveSlaves returns a snapshot of currently-alive slaves.
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

// allSlaves returns a snapshot of all slaves (alive or not).
func allSlaves() []*Slave {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]*Slave, len(Registry))
	copy(out, Registry)
	return out
}

// slaveByID looks up a slave by its ID (lock-safe).
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

// CreateTableMeta records a new table in the shard map, bumps version,
// persists to disk, and asynchronously replicates to all slaves.
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

// DropTableMeta removes a single table from the shard map.
// Called only by /table/drop, NOT by /db/drop (see below).
func DropTableMeta(db, table string) {
	tableMu.Lock()
	delete(tables, tableKey(db, table))
	tableMu.Unlock()

	BumpVersion()
	SaveMetadata()
	go replicateToSlaves()
}

// MarkDBDropped is called by /db/drop.
// It does NOT remove table metadata — the storage layer falls back to the
// replica schema automatically, so queries continue to work.
// The DB name is added to DroppedDBs so slaves can skip the primary attempt.
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

// ── Canonical Snapshot struct ──────────────────────────────────────────────
//
// Snapshot is the single, shared serialisation format used by:
//   - gateway/metadata (persistence + replication to slaves)
//   - slave-go         (receiving /shard/metadata/sync + promoted-gateway save)
//   - /gateway/sync    (promoted slave POSTs this back to restore the gateway)
//
// Keeping one canonical struct prevents the version field being silently
// dropped (the original bug) and ensures DroppedDBs is always propagated.

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

// ── Persistence ────────────────────────────────────────────────────────────

// marshalSnapshot serialises current in-memory state atomically.
func marshalSnapshot() ([]byte, error) {
	// Take registry snapshot under its own lock.
	registryMu.RLock()
	slaves := make([]PersistedSlave, len(Registry))
	for i, s := range Registry {
		slaves[i] = PersistedSlave{ID: s.ID, URL: s.URL}
	}
	registryMu.RUnlock()

	// Take table snapshot under its own lock.
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

// SaveMetadata writes the current snapshot to disk atomically (write to a
// temp file then rename to avoid a torn read on the slave side).
func SaveMetadata() {
	data, err := marshalSnapshot()
	if err != nil {
		log.Printf("[metadata] SaveMetadata: marshal error: %v", err)
		return
	}

	// Atomic write: temp file + rename.
	tmp := MetadataFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("[metadata] SaveMetadata: write tmp error: %v", err)
		return
	}
	if err := os.Rename(tmp, MetadataFile); err != nil {
		log.Printf("[metadata] SaveMetadata: rename error: %v", err)
		return
	}

	// Count tables for the log line (snapshot already taken above).
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

// LoadMetadataFromBytes applies an incoming Snapshot under a version guard.
//
// Rules:
//   - Version == 0  → legacy / first boot; always adopt.
//   - incoming > current → adopt full snapshot (newer state wins).
//   - incoming == current → merge DroppedDBs only (same state, peer may know of new drops).
//   - incoming < current → ignore (we already have something newer).
//
// Slave alive-status is preserved for slaves that are already in the registry;
// newly-seen slaves start as alive=true.
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
		// Same version — only merge DroppedDBs in case the peer knows of extra drops.
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

	// ── Update table map ──────────────────────────────────────────────────
	tableMu.Lock()
	tables = make(map[string]*TableMeta, len(snap.Tables))
	for _, m := range snap.Tables {
		tables[tableKey(m.DB, m.Table)] = m
	}
	tableMu.Unlock()

	// ── Merge slave registry — preserve alive status ───────────────────────
	registryMu.Lock()
	// Build a lookup of existing slaves so we can carry over their alive flag.
	existing := make(map[string]*Slave, len(Registry))
	for _, s := range Registry {
		existing[s.ID] = s
	}
	newRegistry := make([]*Slave, 0, len(snap.Slaves))
	for _, ps := range snap.Slaves {
		if old, ok := existing[ps.ID]; ok {
			// Update URL in case it changed; keep alive status as-is.
			old.URL = ps.URL
			newRegistry = append(newRegistry, old)
		} else {
			// Brand-new slave — default to alive so health checker can verify.
			newRegistry = append(newRegistry, newSlave(ps.ID, ps.URL))
		}
	}
	Registry = newRegistry
	registryMu.Unlock()

	// ── Apply DroppedDBs ──────────────────────────────────────────────────
	setDroppedDBs(snap.DroppedDBs)

	if snap.Version > 0 {
		SetVersion(snap.Version)
	}

	log.Printf("[metadata] loaded (version=%d, tables=%d, slaves=%d, dropped_dbs=%v)",
		snap.Version, len(snap.Tables), len(snap.Slaves), snap.DroppedDBs)
	return nil
}

// CurrentSnapshot returns the current in-memory state as a Snapshot.
// Used by /gateway/status and by the promoted slave's sync handler.
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

// ── Active replication ─────────────────────────────────────────────────────

var replicationClient = &http.Client{Timeout: 5 * time.Second}

// replicateToSlaves pushes the current full Snapshot to EVERY slave.
// Each slave gets up to 3 attempts with 500ms backoff before being marked dead.
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
			// All attempts exhausted — mark slave as potentially dead so the
			// health checker will re-verify on its next tick.
			sl.SetAlive(false)
			log.Printf("[metadata] replicate→%s: all attempts failed; marked OFFLINE", sl.ID)
		}(s)
	}
}

// ReplicateToSlaves is an exported wrapper that triggers active
// replication of the current metadata snapshot to all registered slaves.
func ReplicateToSlaves() {
	go replicateToSlaves()
}

// ── Health checker ─────────────────────────────────────────────────────────

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
						// Slave just recovered — push latest snapshot so it catches up.
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

// ── Routing helpers ────────────────────────────────────────────────────────

// SlaveForID deterministically maps a record id to the shard that owns it.
func (m *TableMeta) SlaveForID(id string) *Slave {
	h := fnv.New32a()
	h.Write([]byte(id))
	idx := int(h.Sum32()) % m.ShardCount
	return slaveByID(m.SlaveIDs[idx])
}

var rrMu sync.Mutex
var rrCounter int

// NextInsertSlave returns the next alive slave for a round-robin insert,
// together with its shard index so the slave can tag the record.
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

package engine

import (
	"sync"
)

const defaultFlushThreshold = 1 * 1024 * 1024 // 1MB

// MemTable holds recent writes in memory.
// All reads check here first before going to SSTables on disk.
// When size exceeds threshold, it's flushed to a new SSTable.
type MemTable struct {
	mu        sync.RWMutex
	data      map[string]string
	tombstone map[string]bool // tracks deletes
	size      int
	threshold int
}

func NewMemTable() *MemTable {
	return &MemTable{
		data:      make(map[string]string),
		tombstone: make(map[string]bool),
		threshold: defaultFlushThreshold,
	}
}

func (m *MemTable) Set(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if old, exists := m.data[key]; exists {
		m.size -= len(old)
	}
	m.data[key] = value
	delete(m.tombstone, key)
	m.size += len(key) + len(value)
}

func (m *MemTable) Get(key string) (string, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.tombstone[key] {
		return "", true, false // deleted
	}
	val, ok := m.data[key]
	return val, false, ok
}

func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if val, exists := m.data[key]; exists {
		m.size -= len(key) + len(val)
		delete(m.data, key)
	}
	m.tombstone[key] = true
}

func (m *MemTable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size >= m.threshold
}

// Snapshot returns a sorted copy of all entries for SSTable flush
func (m *MemTable) Snapshot() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := make(map[string]string, len(m.data))
	for k, v := range m.data {
		snap[k] = v
	}
	for k := range m.tombstone {
		snap[k] = "" // empty string signals tombstone in SSTable
	}
	return snap
}

func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string)
	m.tombstone = make(map[string]bool)
	m.size = 0
}

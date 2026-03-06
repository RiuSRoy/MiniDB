package engine

import (
	"fmt"
	"sync"
)

// DB is the main entry point — coordinates MemTable, WAL, and SSTables
// Read path:  MemTable → SSTables (newest first)
// Write path: WAL → MemTable → (flush to SSTable when threshold hit)
type DB struct {
	mu         sync.RWMutex
	memTable   *MemTable
	wal        *WAL
	sstables   []*SSTable
	dataDir    string
	compactor  *BackgroundCompactor
}

func Open(dataDir string) (*DB, error) {
	wal, err := NewWAL(dataDir + "/wal.log")
	if err != nil {
		return nil, fmt.Errorf("WAL init failed: %w", err)
	}

	mem := NewMemTable()

	// Recover in-memory state from WAL on startup
	entries, err := wal.Recover()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}
	for _, e := range entries {
		switch e.Op {
		case 'S':
			mem.Set(e.Key, e.Value)
		case 'D':
			mem.Delete(e.Key)
		}
	}

	db := &DB{
		memTable: mem,
		wal:      wal,
		dataDir:  dataDir,
	}

	db.compactor = NewBackgroundCompactor(dataDir, &db.sstables)
	db.compactor.Start()

	return db, nil
}

func (db *DB) Set(key, value string) error {
	// 1. WAL first — guarantees durability
	if err := db.wal.Append('S', key, value); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// 2. MemTable
	db.memTable.Set(key, value)

	// 3. Flush to SSTable if MemTable is full
	if db.memTable.ShouldFlush() {
		return db.flush()
	}
	return nil
}

func (db *DB) Get(key string) (string, bool) {
	// Check MemTable first (most recent data)
	val, deleted, found := db.memTable.Get(key)
	if deleted {
		return "", false
	}
	if found {
		return val, true
	}

	// Walk SSTables newest-first
	db.mu.RLock()
	defer db.mu.RUnlock()
	for i := len(db.sstables) - 1; i >= 0; i-- {
		if val, ok := db.sstables[i].Get(key); ok {
			return val, true
		}
	}
	return "", false
}

func (db *DB) Delete(key string) error {
	if err := db.wal.Append('D', key, ""); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}
	db.memTable.Delete(key)
	return nil
}

func (db *DB) flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	snapshot := db.memTable.Snapshot()
	sst, err := FlushToSSTable(db.dataDir, snapshot)
	if err != nil {
		return fmt.Errorf("SSTable flush failed: %w", err)
	}

	db.sstables = append(db.sstables, sst)
	db.memTable.Clear()
	db.wal.Truncate()
	return nil
}

func (db *DB) Close() error {
	db.compactor.Stop()
	return db.wal.Close()
}

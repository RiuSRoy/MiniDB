package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

// SSTableEntry is one record written to disk
type SSTableEntry struct {
	Key     string `json:"k"`
	Value   string `json:"v"`
	Deleted bool   `json:"d,omitempty"`
}

// SSTable is an immutable, sorted file on disk.
// Written once during MemTable flush, never modified.
// Multiple SSTables are merged during compaction.
type SSTable struct {
	Path    string
	Entries []SSTableEntry
}

// Flush writes MemTable snapshot to a new SSTable file
func FlushToSSTable(dir string, snapshot map[string]string) (*SSTable, error) {
	// Sort keys for binary search later
	keys := make([]string, 0, len(snapshot))
	for k := range snapshot {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	path := fmt.Sprintf("%s/sst_%d.sst", dir, time.Now().UnixNano())
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable: %w", err)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	entries := make([]SSTableEntry, 0, len(keys))

	for _, k := range keys {
		v := snapshot[k]
		entry := SSTableEntry{Key: k, Value: v, Deleted: v == ""}
		line, _ := json.Marshal(entry)
		writer.Write(line)
		writer.WriteByte('\n')
		entries = append(entries, entry)
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	return &SSTable{Path: path, Entries: entries}, nil
}

// Get performs binary search on the sorted SSTable entries
func (s *SSTable) Get(key string) (string, bool) {
	idx := sort.Search(len(s.Entries), func(i int) bool {
		return s.Entries[i].Key >= key
	})
	if idx < len(s.Entries) && s.Entries[idx].Key == key {
		if s.Entries[idx].Deleted {
			return "", false
		}
		return s.Entries[idx].Value, true
	}
	return "", false
}

// LoadSSTable reads an SSTable back from disk (used on startup)
func LoadSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []SSTableEntry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var entry SSTableEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return &SSTable{Path: path, Entries: entries}, nil
}

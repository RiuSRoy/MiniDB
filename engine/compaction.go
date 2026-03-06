package engine

import (
	"fmt"
	"os"
	"sort"
	"time"
)

// Compaction merges multiple SSTables into one, removing duplicates
// and tombstones. This keeps read performance high as the number of
// SSTable files grows over time.
//
// This is the background process that makes LSM-trees practical.
// Without it, reads would have to scan every SSTable file.
func Compact(dir string, tables []*SSTable) (*SSTable, error) {
	if len(tables) < 2 {
		return nil, fmt.Errorf("need at least 2 SSTables to compact")
	}

	// Merge all entries — later tables win (newer data)
	merged := make(map[string]SSTableEntry)
	for _, table := range tables {
		for _, entry := range table.Entries {
			merged[entry.Key] = entry
		}
	}

	// Sort keys
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build snapshot — skip tombstones entirely (they've served their purpose)
	snapshot := make(map[string]string)
	for _, k := range keys {
		entry := merged[k]
		if !entry.Deleted {
			snapshot[k] = entry.Value
		}
	}

	// Write new compacted SSTable
	newTable, err := FlushToSSTable(dir, snapshot)
	if err != nil {
		return nil, fmt.Errorf("compaction write failed: %w", err)
	}

	// Delete old SSTable files
	for _, table := range tables {
		os.Remove(table.Path)
	}

	return newTable, nil
}

// BackgroundCompactor runs compaction when SSTable count exceeds threshold
type BackgroundCompactor struct {
	dir       string
	tables    *[]*SSTable
	threshold int
	stop      chan struct{}
}

func NewBackgroundCompactor(dir string, tables *[]*SSTable) *BackgroundCompactor {
	return &BackgroundCompactor{
		dir:       dir,
		tables:    tables,
		threshold: 4, // compact when we have 4+ SSTables
		stop:      make(chan struct{}),
	}
}

func (bc *BackgroundCompactor) Start() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(*bc.tables) >= bc.threshold {
					compacted, err := Compact(bc.dir, *bc.tables)
					if err == nil {
						*bc.tables = []*SSTable{compacted}
					}
				}
			case <-bc.stop:
				return
			}
		}
	}()
}

func (bc *BackgroundCompactor) Stop() {
	close(bc.stop)
}

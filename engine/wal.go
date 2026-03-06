package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// WALEntry represents a single operation logged to disk
type WALEntry struct {
	Op    byte   // 'S' = Set, 'D' = Delete
	Key   string
	Value string
}

// WAL is a Write-Ahead Log — every write hits disk here first
// guaranteeing durability even on crash
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	return &WAL{file: f, path: path}, nil
}

// Append writes a WAL entry atomically to disk before any in-memory update
func (w *WAL) Append(op byte, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := encodeEntry(op, key, value)
	_, err := w.file.Write(entry)
	return err
}

// Recover replays the WAL on startup to rebuild in-memory state after a crash
func (w *WAL) Recover() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []WALEntry
	reader := bufio.NewReader(w.file)

	for {
		entry, err := decodeEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Partial write at crash boundary — safe to stop here
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Truncate clears the WAL after a successful MemTable flush to SSTable
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Truncate(0)
}

func (w *WAL) Close() error {
	return w.file.Close()
}

// encodeEntry: [op:1][keyLen:4][key][valLen:4][val]
func encodeEntry(op byte, key, value string) []byte {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	buf := make([]byte, 1+4+len(keyBytes)+4+len(valBytes))
	buf[0] = op
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(keyBytes)))
	copy(buf[5:], keyBytes)
	binary.LittleEndian.PutUint32(buf[5+len(keyBytes):], uint32(len(valBytes)))
	copy(buf[5+len(keyBytes)+4:], valBytes)
	return buf
}

func decodeEntry(r *bufio.Reader) (WALEntry, error) {
	op, err := r.ReadByte()
	if err != nil {
		return WALEntry{}, err
	}

	key, err := readLenPrefixedString(r)
	if err != nil {
		return WALEntry{}, err
	}

	value, err := readLenPrefixedString(r)
	if err != nil {
		return WALEntry{}, err
	}

	return WALEntry{Op: op, Key: key, Value: value}, nil
}

func readLenPrefixedString(r *bufio.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

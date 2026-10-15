package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Record is one durable change in the write-ahead log.
type Record struct {
	Index int64  `json:"index"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type wal struct {
	file *os.File
}

func openWAL(path string) (*wal, []Record, error) {
	if path == "" {
		return &wal{}, nil, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, nil, err
	}
	var records []Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("read WAL: %w", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return nil, nil, err
	}
	return &wal{file: file}, records, nil
}

func (w *wal) append(record Record) error {
	if w.file == nil {
		return nil
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *wal) close() error {
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

// replay applies records from a reader. It is kept small to make the recovery
// format easy to understand and test.
func replay(r io.Reader, apply func(Record) error) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		if err := apply(record); err != nil {
			return err
		}
	}
	return scanner.Err()
}

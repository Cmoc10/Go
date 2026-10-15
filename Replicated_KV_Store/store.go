package main

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("key not found")

// Store is an in-memory key/value map with a durable, fsync'ed WAL.
type Store struct {
	mu     sync.RWMutex
	values map[string]string
	log    []Record
	next   int64
	wal    *wal
	closed bool
}

func NewStore(walPath string) (*Store, error) {
	w, records, err := openWAL(walPath)
	if err != nil {
		return nil, err
	}
	s := &Store{values: make(map[string]string), wal: w}
	for _, record := range records {
		if record.Index == 0 {
			record.Index = s.next + 1
		}
		if err := s.apply(record); err != nil {
			w.close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	return value, ok
}

func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(Record{Op: "put", Key: key, Value: value})
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(Record{Op: "delete", Key: key})
}

func (s *Store) commitLocked(record Record) error {
	if s.closed {
		return errors.New("store is closed")
	}
	record.Index = s.next + 1
	if err := s.wal.append(record); err != nil {
		return err
	}
	return s.apply(record)
}

func (s *Store) apply(record Record) error {
	if record.Op == "put" {
		s.values[record.Key] = record.Value
	} else if record.Op == "delete" {
		delete(s.values, record.Key)
	} else {
		return errors.New("unknown WAL operation: " + record.Op)
	}
	if record.Index > s.next {
		s.next = record.Index
	}
	s.log = append(s.log, record)
	return nil
}

func (s *Store) Records() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Record(nil), s.log...)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.close()
}

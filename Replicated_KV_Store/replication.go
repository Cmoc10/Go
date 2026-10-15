package main

import (
	"errors"
	"sync"
)

// Replica receives committed records from a leader.
type Replica interface {
	Apply(Record) error
	LastIndex() int64
}

type Follower struct {
	store *Store
	mu    sync.Mutex
	last  int64
}

func NewFollower(store *Store) *Follower {
	var last int64
	for _, record := range store.Records() {
		if record.Index > last {
			last = record.Index
		}
	}
	return &Follower{store: store, last: last}
}

func (f *Follower) Apply(record Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if record.Index <= f.last {
		return nil // replaying an already received record is harmless
	}
	if record.Index != f.last+1 {
		return errors.New("replica is missing an earlier record")
	}
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.store.wal.append(record); err != nil {
		return err
	}
	if err := f.store.apply(record); err != nil {
		return err
	}
	f.last = record.Index
	return nil
}

func (f *Follower) LastIndex() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

// ReplicatedStore adds a deliberately small leader/follower layer to Store.
type ReplicatedStore struct {
	leader    *Store
	mu        sync.Mutex
	followers map[string]Replica
}

func NewReplicatedStore(leader *Store) *ReplicatedStore {
	return &ReplicatedStore{leader: leader, followers: make(map[string]Replica)}
}

func (r *ReplicatedStore) Get(key string) (string, bool) { return r.leader.Get(key) }

func (r *ReplicatedStore) Put(key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.leader.Put(key, value); err != nil {
		return err
	}
	return r.replicateLatestLocked()
}

func (r *ReplicatedStore) Delete(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.leader.Delete(key); err != nil {
		return err
	}
	return r.replicateLatestLocked()
}

func (r *ReplicatedStore) replicateLatestLocked() error {
	records := r.leader.Records()
	record := records[len(records)-1]
	for _, follower := range r.followers {
		if err := follower.Apply(record); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReplicatedStore) AddFollower(name string, follower Replica) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.followers[name]; exists {
		return errors.New("follower already exists")
	}
	for _, record := range r.leader.Records() {
		if record.Index > follower.LastIndex() {
			if err := follower.Apply(record); err != nil {
				return err
			}
		}
	}
	r.followers[name] = follower
	return nil
}

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreReplaysWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.wal")
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("a", "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("a", "two"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok := reopened.Get("a"); !ok || got != "two" {
		t.Fatalf("got %q, %v", got, ok)
	}
	if _, ok := reopened.Get("gone"); ok {
		t.Fatal("deleted key exists")
	}
}

func TestHTTPCRUD(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := httptest.NewServer(NewHandler(store))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPut, server.URL+"/kv/name", strings.NewReader("Ada"))
	response, err := http.DefaultClient.Do(req)
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT: %v, status %d", err, response.StatusCode)
	}
	response.Body.Close()
	response, err = http.Get(server.URL + "/kv/name")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "Ada" {
		t.Fatalf("GET: status %d, body %q", response.StatusCode, body)
	}
	req, _ = http.NewRequest(http.MethodDelete, server.URL+"/kv/name", nil)
	response, err = http.DefaultClient.Do(req)
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %v, status %d", err, response.StatusCode)
	}
	response.Body.Close()
	response, err = http.Get(server.URL + "/kv/name")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing GET status %d", response.StatusCode)
	}
}

func TestReplicationCatchUp(t *testing.T) {
	leader, _ := NewStore("")
	defer leader.Close()
	followerStore, _ := NewStore("")
	defer followerStore.Close()
	replicated := NewReplicatedStore(leader)
	if leader.Put("before", "value") != nil {
		t.Fatal("leader put")
	}
	follower := NewFollower(followerStore)
	if err := replicated.AddFollower("one", follower); err != nil {
		t.Fatal(err)
	}
	if got, ok := followerStore.Get("before"); !ok || got != "value" {
		t.Fatalf("catch-up got %q, %v", got, ok)
	}
	if err := replicated.Put("after", "value2"); err != nil {
		t.Fatal(err)
	}
	if got, ok := followerStore.Get("after"); !ok || got != "value2" {
		t.Fatalf("replication got %q, %v", got, ok)
	}
}

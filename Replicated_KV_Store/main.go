package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	walPath := flag.String("wal", "kv.wal", "path to the write-ahead log")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()
	store, err := NewStore(*walPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	log.Printf("listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, NewHandler(store)))
}

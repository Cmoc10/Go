package main

import (
	"io"
	"net/http"
	"strings"
)

type KV interface {
	Get(string) (string, bool)
	Put(string, string) error
	Delete(string) error
}

func NewHandler(store KV) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		key := strings.TrimPrefix(req.URL.Path, "/kv/")
		if key == "" || key == req.URL.Path {
			http.Error(w, "use /kv/{key}", http.StatusNotFound)
			return
		}
		switch req.Method {
		case http.MethodGet:
			value, ok := store.Get(key)
			if !ok {
				http.NotFound(w, req)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, value)
		case http.MethodPut:
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := store.Put(key, string(body)); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			if err := store.Delete(key); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Header().Set("Allow", "GET, PUT, DELETE")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

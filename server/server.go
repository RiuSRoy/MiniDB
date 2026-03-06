package server

import (
	"encoding/json"
	"net/http"

	"github.com/RiuSRoy/MiniDB/engine"
)

type Server struct {
	db  *engine.DB
	mux *http.ServeMux
}

type setRequest struct {
	Value string `json:"value"`
}

type response struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

func New(db *engine.DB) *Server {
	s := &Server{db: db, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /keys/{key}", s.handleGet)
	s.mux.HandleFunc("POST /keys/{key}", s.handleSet)
	s.mux.HandleFunc("DELETE /keys/{key}", s.handleDelete)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, ok := s.db.Get(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response{Error: "key not found"})
		return
	}
	json.NewEncoder(w).Encode(response{Key: key, Value: val})
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response{Error: "invalid request body"})
		return
	}
	if err := s.db.Set(key, req.Value); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response{Key: key, Value: req.Value})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.db.Delete(key); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

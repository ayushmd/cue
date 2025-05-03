package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Server struct {
	m      *Scheduler
	mu     sync.Mutex
	srv    *http.Server
	nextID int
}

func NewServer() *Server {
	return &Server{
		m:      NewScheduler(),
		nextID: 0,
	}
}

func (s *Server) listen(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Queue name missing", http.StatusBadRequest)
		return
	}
	queueName := parts[3]

	ok := s.m.r.CheckExsists(queueName)
	if !ok {
		http.Error(w, "No queue found", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()

	s.m.r.AddListener(queueName, Listener{
		w:  w,
		id: id,
	})

	notify := r.Context().Done()
	go func() {
		<-notify
		s.m.r.RemoveListener(queueName, id)
		fmt.Println("Client disconnected:", id)
	}()

	select {}
}

type Item struct {
	QueueName string `json:"queueName"`
	Data      any    `json:"data"`
	TTL       int    `json:"ttl"`
	Retries   int    `json:"retries"`
}

func (s *Server) pushItem(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	var data Item
	if err := json.Unmarshal(body, &data); err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	err = s.m.CreateItem(data, body)
	if err != nil {
		http.Error(w, "Failed to create", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type CreateQueueRequest struct {
	QueueName string `json:"queueName"`
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Failed to read body")
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	var data CreateQueueRequest
	if err := json.Unmarshal(body, &data); err != nil {
		fmt.Println("Failed to parse body")
		http.Error(w, "Failed to parse body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	err = s.m.CreateQueue(data.QueueName)
	if err != nil {
		http.Error(w, "Failed to create queue", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	s := NewServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/item/listen/", s.listen)
	mux.HandleFunc("/item/push", s.pushItem)
	mux.HandleFunc("/queue/create", s.createQueue)

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    ":8080",
		Handler: h2c.NewHandler(mux, h2s),
	}
	s.srv = server

	fmt.Println("Starting HTTP/2 (h2c) server on :8080")
	log.Fatal(s.srv.ListenAndServe())
}

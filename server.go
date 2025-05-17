package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	s := &Server{
		m:      NewScheduler(),
		nextID: 0,
	}
	mux := s.RegisterRoutes()
	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    ":8080",
		Handler: h2c.NewHandler(mux, h2s),
	}
	s.srv = server
	return s
}

func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

func (s *Server) RegisterRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/item/listen/", s.listen)
	mux.HandleFunc("/item/push", s.pushItem)
	mux.HandleFunc("/item/ack/", s.ack)
	mux.HandleFunc("/queue/create", s.createQueue)
	mux.HandleFunc("/queue/list", s.listQueues)
	mux.HandleFunc("/queue/delete", s.deleteQueue)
	return mux
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

	// buf := make([]byte, 1024)
	select {}
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

type QueueRequest struct {
	QueueName string `json:"queueName"`
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Failed to read body")
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	var data QueueRequest
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

func (s *Server) deleteQueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Failed to read body")
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	var data QueueRequest
	if err := json.Unmarshal(body, &data); err != nil {
		fmt.Println("Failed to parse body")
		http.Error(w, "Failed to parse body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	err = s.m.DeleteQueue(data.QueueName)
	if err != nil {
		http.Error(w, "Failed to create queue", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type ListQueueRequest struct {
	Data    []string `json:"data"`
	Success bool     `json:"success"`
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	qs := s.m.ListQueues()
	resp, err := json.Marshal(ListQueueRequest{
		Data:    qs,
		Success: true,
	})
	if err != nil {
		http.Error(w, "Failed to create queue", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Queue name missing", http.StatusBadRequest)
		return
	}
	ackid := parts[3]
	num, _ := strconv.ParseInt(ackid, 10, 64)
	s.m.Ack(num)
}

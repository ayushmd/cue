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

	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()
	var que *Queue = nil
	for k, q := range s.m.queues {
		if k == queueName {
			que = q
			break
		}
	}

	if que == nil {
		http.Error(w, "No queue found", http.StatusBadRequest)
		return
	}

	que.mu.Lock()
	que.Listeners = append(que.Listeners, Listener{
		w:  w,
		id: id,
	})
	que.mu.Unlock()

	notify := r.Context().Done()
	go func() {
		<-notify
		que.mu.Lock()
		var ind int = -1
		for i, v := range que.Listeners {
			if v.id == id {
				ind = i
				break
			}
		}
		if ind != -1 {
			que.Listeners = append(que.Listeners[:ind], que.Listeners[ind+1:]...)
		}
		que.mu.Unlock()
		fmt.Println("Client disconnected:", id)
	}()
	select {}
}

type Item struct {
	QueueName string `json:"queueName"`
	Data      any    `json:"data"`
	TTL       int    `json:"ttl"`
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
	qs := s.m.ReturnPossibleQs(data.QueueName)
	if len(qs) == 0 {
		fmt.Fprintln(w, "Queue not found")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = s.m.ds.CreateSync(int64(data.TTL), body)
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
	qs := s.m.ReturnPossibleQs(data.QueueName)
	if len(qs) != 0 {
		fmt.Fprintln(w, "Queue name already exsists")
		w.WriteHeader(http.StatusOK)
		return
	}
	q := NewQueue(data.QueueName)
	s.m.CreateQueue(q)
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

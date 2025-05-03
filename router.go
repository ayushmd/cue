package main

import (
	"fmt"
	"net/http"
	"regexp"
	"sync"
)

type Queue struct {
	mu        sync.Mutex
	Name      string
	ind       int
	Listeners []Listener
}

type QueueExsistsError struct {
}

func (e *QueueExsistsError) Error() string {
	return "Queue Already Exsists"
}

type QueueDoesNotExsists struct {
}

func (e QueueDoesNotExsists) Error() string {
	return "Queue does not exsists"
}

func NewQueue(name string) *Queue {
	return &Queue{
		Name: name,
		ind:  0,
	}
}

type Router struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

func NewRouter() *Router {
	return &Router{
		queues: make(map[string]*Queue),
	}
}

func (r *Router) GetMatchingQueues(pattern string) []*Queue {
	r.mu.RLock()
	defer r.mu.RUnlock()
	arr := make([]*Queue, 0)
	for k, v := range r.queues {
		if k == pattern {
			arr = append(arr, v)
		} else if re := regexp.MustCompile(pattern); re.MatchString(k) {
			arr = append(arr, v)
		}
	}
	return arr
}

func (r *Router) CheckExsists(pattern string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for k := range r.queues {
		if k == pattern {
			return true
		} else if re := regexp.MustCompile(pattern); re.MatchString(k) {
			return true
		}
	}
	return false
}

func (r *Router) CreateQueue(qname string) error {
	if r.CheckExsists(qname) {
		return &QueueExsistsError{}
	}
	q := NewQueue(qname)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queues[qname] = q
	return nil
}

func (r *Router) AddListener(qname string, l Listener) {
	r.mu.RLock()
	q := r.queues[qname]
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	q.Listeners = append(q.Listeners, l)
}

func (r *Router) RemoveListener(qname string, id int) {
	que := r.queues[qname]
	que.mu.Lock()
	defer que.mu.Unlock()
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
}

func (r *Router) NotifyListener(l Listener, data any) {
	flusher, ok := l.w.(http.Flusher)
	if !ok {
		http.Error(l.w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(l.w, data)
	flusher.Flush()
}

type SentItemResponse struct {
	sent      bool
	queueName string
}

func (r *Router) SendItem(pattern string, data any) []SentItemResponse {
	arr := make([]SentItemResponse, 0)
	qs := r.GetMatchingQueues(pattern)
	if len(qs) == 0 {
		return arr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, q := range qs {
		if len(q.Listeners) == 0 {
			arr = append(arr, SentItemResponse{
				queueName: q.Name,
				sent:      false,
			})
		} else {
			q.ind = (q.ind + 1) % len(q.Listeners)
			l := q.Listeners[q.ind]
			go r.NotifyListener(l, data)
		}
	}
	return arr
}

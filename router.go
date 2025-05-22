package main

import (
	"fmt"
	"regexp"
	"sync"
)

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

func (r *Router) initQueues(qs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, q := range qs {
		r.queues[q] = NewQueue(q)
	}
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

func (r *Router) DeleteQueue(qname string) error {
	if !r.CheckExsists(qname) {
		return &QueueDoesNotExsists{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.queues, qname)
	return nil
}

func (r *Router) ListQueues() []string {
	arr := make([]string, 0)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, q := range r.queues {
		arr = append(arr, q.Name)
	}
	return arr
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

type NotifyRequest struct {
	Id   int64 `json:"id"`
	Data any   `json:"data"`
}

func (r *Router) NotifyListener(l Listener, id int64, data []byte) {
	// flusher, ok := l.w.(http.Flusher)
	// if !ok {
	// 	http.Error(l.w, "Streaming unsupported", http.StatusInternalServerError)
	// 	return
	// }
	// sendData, _ := json.Marshal(NotifyRequest{
	// 	Id:   id,
	// 	Data: data,
	// })
	// l.w.Header().Set("Content-Type", "application/json")
	// l.w.Header().Set("Cache-Control", "no-cache")
	// fmt.Println("Sending data: ", string(sendData))
	// fmt.Fprintln(l.w, sendData)
	// flusher.Flush()
	l.send(id, data)
}

type SentItemResponse struct {
	sent      bool
	queueName string
}

func (r *Router) SendItem(item Item) []SentItemResponse {
	arr := make([]SentItemResponse, 0)
	qs := r.GetMatchingQueues(item.QueueName)
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
			// go r.NotifyListener(l, item.Id, item.Data)
			fmt.Println("Sending data: ", item.Id)
			go l.send(item.Id, item.Data)
		}
	}
	return arr
}

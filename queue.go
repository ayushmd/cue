package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Listener struct {
	w  http.ResponseWriter
	id int
}

type Queue struct {
	mu        sync.Mutex
	Name      string
	ind       int
	Listeners []Listener
}

func NewQueue(name string) *Queue {
	return &Queue{
		Name: name,
		ind:  0,
	}
}

type Master struct {
	sendCh chan []byte
	queues map[string]*Queue
	ds     *DataStorage
	pq     *PriorityQueue
}

func NewMaster() *Master {
	m := &Master{
		sendCh: make(chan []byte),
		queues: make(map[string]*Queue),
		ds:     NewDataStorage(),
		pq:     NewPriorityQueue(),
	}
	go m.Poll()
	return m
}

func (m *Master) Poll() {
	ticker := time.NewTicker(5 * time.Second)
	lister := time.NewTicker(20 * time.Second)
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixMilli()
			top, _ := m.ds.PeekTTL()
			fmt.Println("The Top is: ", top, now)
			if now >= top {
				items, err := m.ds.ItemsSized(10)
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println("Total items: ", len(items))
				if len(items) != 0 {
					m.ds.DeleteItemRange(int64(items[0].TTL), int64(items[len(items)-1].TTL))
					for _, item := range items {
						m.pq.Push(item, int64(item.TTL))
					}
				}
			}
		case <-lister.C:
			fmt.Println("\nPrinting DB:")
			it, _ := m.ds.db.NewIter(nil)
			for it.First(); it.Valid(); it.Next() {
				fmt.Println("Key: ", string(it.Key()), " Value: ", string(it.Value()))
			}
		case job := <-m.pq.Subscribe():
			iobj := job.(*Item)
			q, ok := m.queues[iobj.QueueName]
			if ok {
				q.mu.Lock()
				flusher, ok := q.Listeners[q.ind].w.(http.Flusher)
				if !ok {
					http.Error(q.Listeners[q.ind].w, "Streaming unsupported", http.StatusInternalServerError)
					return
				}
				fmt.Fprintln(q.Listeners[q.ind].w, iobj.Data)
				flusher.Flush()
				q.ind = (q.ind + 1) % len(q.Listeners)
				q.mu.Unlock()
			}
		}
	}
}

func (m *Master) ReturnPossibleQs(pattern string) []*Queue {
	arr := make([]*Queue, 0)
	for k, v := range m.queues {
		if k == pattern {
			arr = append(arr, v)
		}
	}
	return arr
}

func (m *Master) CreateQueue(q *Queue) {
	m.queues[q.Name] = q
	m.ds.CreateQueue(q.Name)
}

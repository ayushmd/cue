package main

import (
	"container/heap"
	"sync"
	"time"
)

type element struct {
	priority int64
	data     any
	index    int
}

type store []*element

func (s *store) Len() int {
	return len(*s)
}

func (s *store) Less(i, j int) bool {
	h := *s
	return h[i].priority < h[j].priority
}

func (s *store) Swap(i, j int) {
	h := *s
	h[j], h[i] = h[i], h[j]
	h[i].index = i
	h[j].index = j
}

func (s *store) Push(x any) {
	h := *s
	h = append(h, x.(*element))
	*s = h
}

func (s *store) Pop() any {
	h := *s
	n := len(h)
	if len(h) > 0 {
		el := h[n-1]
		*s = h[0 : n-1]
		return el
	}
	return nil
}

type Queue struct {
	mux   *sync.Mutex
	store store
	sub   chan any
}

func NewQueue() *Queue {
	q := &Queue{
		mux:   &sync.Mutex{},
		store: make([]*element, 0),
		sub:   make(chan any, 100),
	}
	heap.Init(&q.store)
	go q.CollectTTL()
	return q
}

func (q *Queue) Subscribe() chan any {
	return q.sub
}

func (q *Queue) CollectTTL() {
	for {
		q.mux.Lock()
		if q.store.Len() > 0 {
			time.Sleep(time.Until(time.Unix(q.store[0].priority, 0)))
			q.mux.Unlock()
			q.sub <- q.Pop()
		} else {
			q.mux.Unlock()
		}
	}
}

func (q *Queue) Push(data any, priority int64) {
	// append the element to the store
	el := &element{
		priority: priority,
		data:     data,
		index:    q.store.Len(),
	}
	q.mux.Lock()
	defer q.mux.Unlock()
	heap.Push(&q.store, el)
	// fix the store order
	heap.Fix(&q.store, el.index)
}

func (q *Queue) Pop() any {
	q.mux.Lock()
	defer q.mux.Unlock()
	if q.store.Len() == 0 {
		return nil
	}

	el := heap.Pop(&q.store)
	if el == nil {
		return nil
	}

	return el.(*element).data
}

// type TTLItem struct {
// 	id        int
// 	createdAt int64
// }

// func main() {
// 	ttlq := NewTTLQueue()

// 	// Background queue listner
// 	go func() {
// 		for {
// 			select {
// 			case job := <-ttlq.Subscribe():
// 				jobj := job.(*TTLItem)
// 				fmt.Printf("Recieved Job %d: Created At: %d Recieved At: %d\n", jobj.id, jobj.createdAt, time.Now().Unix())
// 			}
// 		}
// 	}()

// 	ttlq.Push(&TTLItem{
// 		id:        1,
// 		createdAt: time.Now().Unix(),
// 	}, time.Now().Add(10*time.Second).Unix())

// 	ttlq.Push(&TTLItem{
// 		id:        2,
// 		createdAt: time.Now().Unix(),
// 	}, time.Now().Add(5*time.Second).Unix())

// 	ttlq.Push(&TTLItem{
// 		id:        3,
// 		createdAt: time.Now().Unix(),
// 	}, time.Now().Add(2*time.Second).Unix())
// 	select {}
// }

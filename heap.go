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

type PriorityQueue struct {
	mux   *sync.Mutex
	store store
	sub   chan any
}

func NewPriorityQueue() *PriorityQueue {
	q := &PriorityQueue{
		mux:   &sync.Mutex{},
		store: make([]*element, 0),
		sub:   make(chan any, 100),
	}
	heap.Init(&q.store)
	go q.CollectTTL()
	return q
}

func (q *PriorityQueue) Subscribe() chan any {
	return q.sub
}

func (q *PriorityQueue) CollectTTL() {
	for {
		q.mux.Lock()
		if q.store.Len() > 0 {
			now := time.Now().UnixMilli()
			next := q.store[0].priority

			if next <= now {
				// Ready to pop
				data := q.popLocked()
				q.mux.Unlock()
				q.sub <- data
				continue
			}
			// else {
			// 	// Sleep until the next event
			// 	sleepDuration := time.Until(time.Unix(next, 0))
			// 	if sleepDuration > 0 {
			// 		q.mux.Unlock()
			// 		time.Sleep(sleepDuration)
			// 		continue
			// 	}
			// }
		}
		q.mux.Unlock()

		// Sleep a bit to avoid busy spin if queue is empty
		time.Sleep(5 * time.Millisecond)
		// runtime.Gosched()
	}
}

func (q *PriorityQueue) Push(data any, priority int64) {
	el := &element{
		priority: priority,
		data:     data,
		index:    0,
	}
	q.mux.Lock()
	defer q.mux.Unlock()
	heap.Push(&q.store, el)
}

func (q *PriorityQueue) Pop() any {
	q.mux.Lock()
	defer q.mux.Unlock()
	return q.popLocked()
}

// Only call inside a locked context
func (q *PriorityQueue) popLocked() any {
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
// 	ttlq := NewTTLPriorityQueue()

// 	// Background PriorityQueue listner
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

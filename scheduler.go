package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cockroachdb/pebble"
)

type Listener struct {
	w  http.ResponseWriter
	id int
}

const ZombieTTL int64 = 5

type Scheduler struct {
	r  *Router
	ds *DataStorage
	pq *PriorityQueue
	zq *PriorityQueue
	ch chan Item
}

func NewScheduler() *Scheduler {
	m := &Scheduler{
		ds: NewDataStorage(),
		pq: NewPriorityQueue(),
		zq: NewPriorityQueue(),
		ch: make(chan Item, 1000),
	}
	go m.Sender()
	go m.Poll()
	return m
}

type ZombifiedItem struct {
}

func (s *Scheduler) Sender() {
	for i := range s.ch {
		go s.ConsumeItem(i)
	}
}

func (m *Scheduler) Poll() {
	it := time.NewTicker(500 * time.Millisecond)
	zit := time.NewTicker(500 * time.Millisecond)
	lister := time.NewTicker(20 * time.Second)

	for {
		select {
		case <-it.C:
			m.Peek()
		case <-zit.C:
		case job := <-m.pq.Subscribe():
			go m.ConsumeItem(job.(Item))
		case <-lister.C:
			fmt.Println("\nPrinting DB:")
			it, _ := m.ds.db.NewIter(nil)
			for it.First(); it.Valid(); it.Next() {
				fmt.Println("Key: ", string(it.Key()), " Value: ", string(it.Value()))
			}
		}
	}
}

func (s *Scheduler) ConsumeItem(item Item) {
	arr := s.r.SendItem(item.QueueName, item.Data)
	(&item).Retries = item.Retries - 1
	if len(arr) == 0 {
		if item.Retries >= 0 {
			s.ds.CreateZombieItem(item)
		} else {
			s.ds.CreateDeadItem(item)
		}
	}
	if ZombieWhenAllPatternNotMatch {
		b := s.ds.NewBatch()
		defer b.Close()
		for _, q := range arr {
			(&item).QueueName = q.queueName
			if !q.sent {
				if item.Retries >= 0 {
					s.ds.BatchCreateZombieItem(b, item)
				} else {
					s.ds.BatchCreateDeadItem(b, item)
				}
			}
		}
		err := b.Commit(pebble.Sync)
		if err != nil {
			fmt.Println("Failed to commit")
		}
	}
}

func (m *Scheduler) PutToPQ() {
	defer m.ds.Unlock()
	items, err := m.ds.ItemsSized(10)
	if err != nil {
		fmt.Println(err)
	}
	if len(items) != 0 {
		b := m.ds.NewBatch()
		for _, item := range items {
			m.pq.Push(item, int64(item.TTL))
			(&item).Retries = MaxZombiefiedRetries
			m.ds.BatchCreateZombieItem(b, item)
		}
		if err := b.Commit(pebble.Sync); err != nil {
			fmt.Println("Error in commiting")
		}
		b.Close()
		m.ds.DeleteItemRange(int64(items[0].TTL), int64(items[len(items)-1].TTL))
	}
}

func (m *Scheduler) Peek() {
	now := time.Now().UnixMilli()
	top, _ := m.ds.PeekTTL()
	if now >= top && top != 0 {
		if m.ds.TryLock() {
			go m.PutToPQ()
		} else {
			fmt.Println("Already running")
		}
	}
}

func (m *Scheduler) PeekZombie() {
	now := time.Now().UnixMilli()
	item, _ := m.ds.PeekZombieItem()
	if now >= int64(item.TTL) && item.TTL != 0 {
		if m.ds.ZTryLock() {
			defer m.ds.ZUnlock()
			go m.ConsumeItem(item)
		} else {
			fmt.Println("Already running")
		}
	}
}

func (m *Scheduler) CreateQueue(qname string) error {
	err := m.r.CreateQueue(qname)
	if err != nil {
		return err
	}
	return m.ds.CreateQueue(qname)
}

func (s *Scheduler) CreateItem(item Item, data []byte) error {
	if !s.r.CheckExsists(item.QueueName) {
		return &QueueDoesNotExsists{}
	}
	now := time.Now().UnixMilli()
	if int64(item.TTL) < now {
		(&item).Retries = MaxZombiefiedRetries
		s.ch <- item
		return nil
	}
	return s.ds.CreateSync(int64(item.TTL), data)
}

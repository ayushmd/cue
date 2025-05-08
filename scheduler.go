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
		r:  NewRouter(),
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
	lister := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-it.C:
			m.Peek()
		case <-zit.C:
			m.PeekZombie()
		case job := <-m.pq.Subscribe():
			item := job.(Item)
			// go m.ConsumeItem(job.(Item))
			go m.r.SendItem(item.QueueName, item.Data)
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
	fmt.Println("Number of retires before: ", item.Retries)
	(&item).Retries = item.Retries - 1
	fmt.Println("Number of retires left: ", item.Retries)
	b := s.ds.NewBatch()
	defer b.Close()
	if len(arr) == 0 {
		if item.Retries > 0 {
			s.ds.BatchDeleteZombieItem(b, item)
			(&item).TTL = int(time.Now().Add(5 * time.Second).UnixMilli())
			s.ds.BatchCreateZombieItem(b, item)
		} else {
			s.ds.BatchDeleteZombieItem(b, item)
			s.ds.BatchCreateDeadItem(b, item)
		}
	}
	if ZombieWhenAllPatternNotMatch {
		for _, q := range arr {
			(&item).QueueName = q.queueName
			if !q.sent {
				if item.Retries >= 0 {
					(&item).TTL = int(time.Now().Add(5 * time.Second).UnixMilli())
					s.ds.BatchCreateZombieItem(b, item)
				} else {
					s.ds.BatchCreateDeadItem(b, item)
					s.ds.BatchDeleteZombieItem(b, item)
				}
			}
		}
	}
	err := b.Commit(pebble.Sync)
	if err != nil {
		fmt.Println("Failed to commit")
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
			m.ds.BatchDeleteItem(b, item)
			(&item).Retries = MaxZombiefiedRetries
			ttlTime := time.UnixMilli(int64(item.TTL))
			delta := time.Until(ttlTime)
			addup := time.Now().Add(5 * time.Second)
			if delta > 0 {
				addup = addup.Add(delta * time.Second)
			}
			(&item).TTL = int(addup.UnixMilli())
			qs := m.r.GetMatchingQueues(item.QueueName)
			for _, q := range qs {
				(&item).QueueName = q.Name
				m.ds.BatchCreateZombieItem(b, item)
			}
		}
		if err := b.Commit(pebble.Sync); err != nil {
			fmt.Println("Error in commiting")
		}
		// m.ds.DeleteItemRange(int64(items[0].TTL), int64(items[len(items)-1].TTL))
		b.Close()
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
			m.ConsumeItem(item)
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
	item.Id = now
	if int64(item.TTL) < now {
		(&item).Retries = MaxZombiefiedRetries
		s.ch <- item
		return nil
	}
	return s.ds.CreateItemSync(item)
}

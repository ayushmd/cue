package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

type Listener struct {
	send func(id int64, data []byte) error
	id   int
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
	qs, err := m.ds.GetQueues()
	if err == nil {
		m.r.initQueues(qs)
	}
	go m.Sender()
	go m.Poll()
	return m
}

type ZombifiedItem struct {
}

func (s *Scheduler) Sender() {
	for item := range s.ch {
		go s.InstantConsume(item)
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
			go m.r.SendItem(item)
		case <-lister.C:
			fmt.Println("\nPrinting DB:")
			it, _ := m.ds.db.NewIter(nil)
			for it.First(); it.Valid(); it.Next() {
				if len(it.Value()) > 20 {
					fmt.Println("Key: ", string(it.Key()), " Value: ", string(it.Value()[:20]))
				} else {
					fmt.Println("Key: ", string(it.Key()), " Value: ", string(it.Value()))
				}
			}
		}
	}
}

func IsNoneSend(arr []SentItemResponse) bool {
	for _, res := range arr {
		if res.sent {
			return false
		}
	}
	return true
}

func (s *Scheduler) Retry(b *pebble.Batch, acked bool, item Item) {
	s.ds.BatchDeleteZombieItem(b, item)
	if acked || item.Retries <= 0 {
		s.ds.BatchCreateDeadItem(b, item)
	} else {
		(&item).TTL = time.Now().Add(5 * time.Second).UnixMilli()
		s.ds.BatchCreateZombieItem(b, item)
	}
}

func (s *Scheduler) InstantConsume(item Item) {
	s.r.SendItem(item)
}

func (s *Scheduler) ConsumeItem(item Item) {
	exsists, _ := s.ds.CheckAckDeleteExsists(item.Id)
	if s.ds.ZTryLock() {
		defer s.ds.ZUnlock()
		arr := make([]SentItemResponse, 0)
		if !exsists {
			arr = s.r.SendItem(item)
		}
		fmt.Println("Number of retires before: ", item.Retries)
		(&item).Retries = item.Retries - 1
		fmt.Println("Number of retires left: ", item.Retries)
		b := s.ds.NewBatch()
		defer b.Close()
		if len(arr) == 0 || IsNoneSend(arr) {
			// fmt.Println("Retrying ", arr)
			s.Retry(b, exsists, item)
		}
		if ZombieWhenAllPatternNotMatch {
			for _, q := range arr {
				(&item).QueueName = q.queueName
				if !q.sent {
					s.Retry(b, exsists, item)
				}
			}
		}
		err := b.Commit(pebble.NoSync)
		if err != nil {
			fmt.Println("Failed to commit")
		}
	}
}

func (m *Scheduler) PopItem(b *pebble.Batch, item Item, exsists bool) {
	m.pq.Push(item, int64(item.TTL))
	if exsists {
		m.ds.BatchDeleteItem(b, item)
	}
	ttlTime := time.UnixMilli(item.TTL)
	delta := time.Until(ttlTime)
	addup := time.Now().Add(5 * time.Second)
	if delta > 0 {
		addup = addup.Add(delta)
	}
	(&item).TTL = addup.UnixMilli()
	m.ZombifyAllMatching(b, item)
}

func (s *Scheduler) ZombifyAllMatching(b *pebble.Batch, item Item) {
	(&item).Retries = MaxZombiefiedRetries
	qs := s.r.GetMatchingQueues(item.QueueName)
	for _, q := range qs {
		(&item).QueueName = q.Name
		s.ds.BatchCreateZombieItem(b, item)
	}
}

func (m *Scheduler) PutToPQ() {
	// defer m.ds.Unlock()
	items, err := m.ds.ItemsSized(10)
	if err != nil {
		fmt.Println(err)
	}
	if len(items) != 0 {
		b := m.ds.NewBatch()
		for _, item := range items {
			m.PopItem(b, item, true)
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			fmt.Println("Error in commiting")
		}
		b.Close()
	}
}

func (m *Scheduler) Peek() {
	now := time.Now().UnixMilli()
	top, _ := m.ds.PeekTTL()
	if now >= top && top != 0 {
		// if m.ds.TryLock() {
		m.PutToPQ()
		// } else {
		// 	fmt.Println("Already running")
		// }
	}
}

func (m *Scheduler) PeekZombie() {
	now := time.Now().UnixMilli()
	item, _ := m.ds.PeekZombieItem()
	if now >= int64(item.TTL) && item.TTL != 0 {
		// if m.ds.ZTryLock() {
		// 	defer m.ds.ZUnlock()
		m.ConsumeItem(item)
		// } else {
		// 	fmt.Println("Already running")
		// }
	}
}

func (m *Scheduler) CreateQueue(qname string) error {
	err := m.r.CreateQueue(qname)
	if err != nil {
		var qe *QueueExsistsError
		if errors.As(err, &qe) {
			return nil
		}
		return err
	}
	return m.ds.CreateQueue(qname)
}

func (s *Scheduler) CreateItem(item Item) error {
	if !s.r.CheckExsists(item.QueueName) {
		return &QueueDoesNotExsists{}
	}
	now := time.Now().UnixMilli()
	item.Id = now
	if int64(item.TTL) <= now {
		s.ch <- item
		b := s.ds.NewBatch()
		defer b.Close()
		s.ZombifyAllMatching(b, item)
		if err := b.Commit(pebble.NoSync); err != nil {
			fmt.Println("Failed to commit")
		}
		return nil
	} else if int64(item.TTL)-now <= PriorityQMainQDiff {
		b := s.ds.NewBatch()
		defer b.Close()
		s.PopItem(b, item, false)
		if err := b.Commit(pebble.NoSync); err != nil {
			fmt.Println("Failed to commit: ", err)
		}
		return nil
	}
	return s.ds.CreateItemSync(item)
}

func (s *Scheduler) Ack(id int64) error {
	return s.ds.CreateAck(id)
}

func (s *Scheduler) ListQueues() []string {
	return s.r.ListQueues()
}

func (s *Scheduler) DeleteQueue(qname string) error {
	err := s.r.DeleteQueue(qname)
	if err != nil {
		return err
	}
	return s.ds.DeleteQueue(qname)
}

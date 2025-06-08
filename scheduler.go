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
	j  *Janitor
	ch chan Item

	itemTick *time.Ticker
	itemInd  int
	zomTick  *time.Ticker
	zomInd   int
}

var poolTime []int64 = []int64{30, 60, 600, 3600, 7200}   // in sec
var peekTime []int64 = []int64{50, 100, 1000, 5000, 8000} // in ms

func NewScheduler() *Scheduler {
	r := NewRouter()
	ds := NewDataStorage("demo")
	j := NewJanitor(r, ds)
	m := &Scheduler{
		ds:       ds,
		pq:       NewPriorityQueue(),
		zq:       NewPriorityQueue(),
		ch:       make(chan Item, 1000),
		r:        r,
		j:        j,
		itemTick: time.NewTicker(50 * time.Millisecond),
		itemInd:  0,
		zomTick:  time.NewTicker(500 * time.Millisecond),
		zomInd:   0,
	}
	qs, err := m.ds.GetQueues()
	if err == nil {
		m.r.initQueues(qs)
	}
	m.Poll()
	return m
}

type ZombifiedItem struct {
}

func (s *Scheduler) Close() {
	s.ds.db.Close()
	s.r.Close()
}

func (s *Scheduler) poolInstant() {
	for item := range s.ch {
		s.r.SendItem(item)
	}
}

func (m *Scheduler) poolItems() {
	// ticker := time.NewTicker(50 * time.Millisecond)
	defer m.itemTick.Stop()

	for range m.itemTick.C {
		m.Peek()
	}
}

func (m *Scheduler) poolZombie() {
	// ticker := time.NewTicker(500 * time.Millisecond)
	defer m.zomTick.Stop()

	for range m.zomTick.C {
		m.PeekZombie()
	}
}

func (s *Scheduler) poolJanitor() {
	ticker := time.NewTicker(time.Duration(cfg.CleanupTimeout) * time.Second)
	for range ticker.C {
		s.j.RunJanitor(janitorOptions{removeall: true, send: false})
	}
}

func (s *Scheduler) InitConnection() {
	if cfg.ReadTimedOutAfterConnecting {
		s.j.RunJanitor(janitorOptions{removeall: false, send: true})
	}
}

func (m *Scheduler) listDb() {
	ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("\nPrinting DB:")
		it, _ := m.ds.db.NewIter(nil)
		for it.First(); it.Valid(); it.Next() {
			val := it.Value()
			if len(val) > 20 {
				fmt.Printf("Key: %s, Value: %s...\n", it.Key(), val[:20])
			} else {
				fmt.Printf("Key: %s, Value: %s\n", it.Key(), val)
			}
		}
	}
}

func (m *Scheduler) poolPriorityQueue() {
	for job := range m.pq.Subscribe() {
		item := job.(Item)
		m.r.SendItem(item)
	}
}

func (m *Scheduler) Poll() {
	go m.poolItems()
	go m.poolZombie()
	go m.poolPriorityQueue()
	go m.poolInstant()
	go m.poolJanitor()
	if debug {
		go m.listDb()
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
		(&item).TTL = time.Now().Add(time.Duration(cfg.RetryAfterTimeout) * time.Second).UnixMilli()
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
		if debug {
			fmt.Println("Number of retires before: ", item.Retries)
		}
		(&item).Retries = item.Retries - 1
		if debug {
			fmt.Println("Number of retires left: ", item.Retries)
		}
		b := s.ds.NewBatch()
		defer b.Close()
		if len(arr) == 0 || IsNoneSend(arr) {
			s.Retry(b, exsists, item)
		}
		if cfg.ZombieWhenAllPatternNotMatch {
			for _, q := range arr {
				(&item).QueueName = q.queueName
				if !q.sent {
					s.Retry(b, exsists, item)
				}
			}
		}
		err := b.Commit(pebble.Sync)
		if err != nil && debug {
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
	(&item).Retries = cfg.MaxZombifiedRetries
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
		if err := b.Commit(pebble.Sync); err != nil && debug {
			fmt.Println("Error in commiting")
		}
		b.Close()
	}
}

func (s *Scheduler) ItemBackoff(top int64) {
	now := time.Now().UnixMilli()
	if top > now {
		diff := top - now
		if diff > poolTime[s.itemInd] {
			// inc
			if s.itemInd < len(poolTime)-1 {
				s.itemInd++
				s.itemTick.Reset(time.Duration(peekTime[s.itemInd]) * time.Millisecond)
			}
		} else {
			// dec
			if s.itemInd > 0 {
				s.itemInd--
				s.itemTick.Reset(time.Duration(peekTime[s.itemInd]) * time.Millisecond)
			}
		}
	} else if top == 0 {
		// inc
		if s.itemInd < len(poolTime)-1 {
			s.itemInd++
			s.itemTick.Reset(time.Duration(peekTime[s.itemInd]) * time.Millisecond)
		}
	}
}

func (s *Scheduler) ItemTickReset() {
	s.itemInd = 0
	s.itemTick.Reset(time.Duration(peekTime[0]) * time.Millisecond)
}

func (m *Scheduler) Peek() {
	now := time.Now().UnixMilli()
	top, _ := m.ds.PeekTTL()
	if (now >= top || top-now <= cfg.PriorityQMainQDiff) && top != 0 {
		// if m.ds.TryLock() {
		m.PutToPQ()
		// } else {
		// 	fmt.Println("Already running")
		// }
	}
	m.ItemBackoff(top)
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
	if !s.r.CheckPatternExsists(item.QueueName) {
		return &QueueDoesNotExsists{}
	}
	now := time.Now().UnixMilli()
	item.Id = now
	if int64(item.TTL) <= now {
		s.ch <- item
		b := s.ds.NewBatch()
		defer b.Close()
		s.ZombifyAllMatching(b, item)
		if err := b.Commit(pebble.Sync); err != nil && debug {
			fmt.Println("Failed to commit")
		}
		return nil
	} else if int64(item.TTL)-now <= cfg.PriorityQMainQDiff {
		b := s.ds.NewBatch()
		defer b.Close()
		s.PopItem(b, item, false)
		if err := b.Commit(pebble.Sync); err != nil && debug {
			fmt.Println("Failed to commit: ", err)
		}
		return nil
	}
	s.ItemTickReset()
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

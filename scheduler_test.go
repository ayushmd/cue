package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ayushmd/delayedQ/pkg/cuecl"
)

var client *cuecl.SchedulerClient

func TestMain(m *testing.M) {
	go func() {
		s := NewServer()
		log.Fatal(s.Start())
	}()
	cli, _ := cuecl.NewSchedulerClient("localhost:8080")
	client = cli
	// Wait until server is ready
	waitUntilServerReady()

	// Run tests
	code := m.Run()

	os.Exit(code)
}

func waitUntilServerReady() {
	for i := 0; i < 20; i++ {
		ok, _ := client.Ping()
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal("Server did not become ready in time")
}

type TestCaseItem struct {
	ID  int    `json:"id"`
	TTL int64  `json:"ttl"`
	Msg string `json:"msg"`
}

func isRecent(ts int64) bool {
	now := time.Now().UnixMilli()
	diff := now - ts
	if diff < 0 {
		diff = -diff
	}
	return diff < 100
}

func TestSimpleScheduling(t *testing.T) {
	var wg sync.WaitGroup
	debug = false
	client.CreateQueue("test")

	cases := map[int]TestCaseItem{
		1: {ID: 1, TTL: time.Now().Add(100 * time.Millisecond).UnixMilli(), Msg: "hola"},
		2: {ID: 2, TTL: time.Now().Add(1 * time.Second).UnixMilli(), Msg: "hi"},
		3: {ID: 3, TTL: time.Now().Add(9 * time.Second).UnixMilli(), Msg: "how"},
	}

	wg.Add(len(cases))
	var mu sync.Mutex
	var errs []string

	go func() {
		ch, _ := client.Listen("test")
		for msg := range ch {
			var item TestCaseItem
			if err := json.Unmarshal(msg, &item); err != nil {
				mu.Lock()
				errs = append(errs, "failed to unmarshal: "+err.Error())
				mu.Unlock()
				wg.Done()
				continue
			}

			tcase := cases[item.ID]
			if !isRecent(tcase.TTL) {
				mu.Lock()
				errs = append(errs,
					fmt.Sprintf("TTL requirement failed for id %d: now=%d, ttl=%d", item.ID, time.Now().UnixMilli(), tcase.TTL))
				mu.Unlock()
			}
			wg.Done()
		}
	}()

	for _, tcase := range cases {
		data, _ := json.Marshal(tcase)
		client.PushItem("test", data, tcase.TTL)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		defer mu.Unlock()
		for _, err := range errs {
			t.Error(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Timed out waiting for all messages")
	}
}

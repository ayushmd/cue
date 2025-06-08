package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

type Janitor struct {
	ds       *DataStorage
	r        *Router
	cleaning int32
	sending  int32
}

func NewJanitor(router *Router, ds *DataStorage) *Janitor {
	return &Janitor{
		ds:       ds,
		r:        router,
		cleaning: 0,
		sending:  0,
	}
}

type janitorOptions struct {
	send      bool
	removeall bool
}

var clean janitorOptions = janitorOptions{removeall: false, send: false}
var join janitorOptions = janitorOptions{removeall: true, send: true}

func (j *Janitor) runJanitor(opts janitorOptions) {
	if opts.send {
		atomic.StoreInt32(&j.sending, 1)
		defer atomic.StoreInt32(&j.sending, 0)
	}
	if atomic.LoadInt32(&j.cleaning) == 0 {
		b := j.ds.NewBatch()
		defer b.Commit(pebble.Sync)
		now := time.Now().UnixMilli()
		atomic.StoreInt32(&j.cleaning, 1)
		defer atomic.StoreInt32(&j.cleaning, 0)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := j.ds.CleanupDeadItems(ctx)
		if err != nil {
			cancel()
			return
		}
		for item := range ch {
			if item.Id != 0 {
				if atomic.LoadInt32(&j.sending) == 1 {
					j.r.SendItem(item, false)
				}
				if opts.removeall {
					j.ds.BatchDeleteDeadItem(b, item.Id)
				} else {
					if now-item.TTL >= cfg.CleanupTimeout {
						j.ds.BatchDeleteDeadItem(b, item.Id)
					} else {
						cancel()
					}
				}
			}
		}
	}
}

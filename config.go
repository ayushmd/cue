package main

import (
	"github.com/ayushmd/delayedQ/pkg/config"
)

var (
	cfg *config.Config
)

func init() {
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		panic(err)
	}
}

// The difference when item is sent directly to priority queue
// instead of db
// const PriorityQMainQDiff int64 = 9000 // 9 seconds

// The pattern when sending out for listener
// false - If sent to even one queue other queue patterns are ignored
//
//	and not zombified
//
// true - Zombifies the item for queue which the listener not available
// const ZombieWhenAllPatternNotMatch bool = false

// // The number of retries to send to zombified
// const MaxZombiefiedRetries = 2

// // The time after which the retry is performed
// const RetryAfterTimeout = 10

// const ReadTimedOutAfterConnecting bool = true

// const CleanupTimeout = 24 * 60 * 60 * 1000

// const Port = 8080

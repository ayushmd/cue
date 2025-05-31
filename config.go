package main

// The difference when item is sent directly to priority queue
// instead of db
const PriorityQMainQDiff int64 = 9 * 1000 // 9 seconds

// The pattern when sending out for listener
// false - If sent to even one queue other queue patterns are ignored
// 		   and not zombified
// true - Zombifies the item for queue which the listener not available
const ZombieWhenAllPatternNotMatch bool = false

// The number of retries to send to zombified
const MaxZombiefiedRetries = 2

// The time after which the retry is performed
const RetryAfterTimeout = 10

const ReadTimedOutAfterConnecting bool = false

const Port = 8080

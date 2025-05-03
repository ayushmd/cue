package main

// The difference when item is sent directly to priority queue
// instead of db
const PriorityQMainQDiff int = 9 * 1000 // 9 seconds

// The pattern when sending out for listener
// false - If sent to even one queue other queue patterns are ignored
// 		   and not zombified
// true - Zombifies the item for queue which the listener not available
const ZombieWhenAllPatternNotMatch = false

// The number of retries to send to zombified
const MaxZombiefiedRetries = 2

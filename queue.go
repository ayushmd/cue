package main

import "sync"

type Queue struct {
	mu        sync.Mutex
	Name      string
	ind       int
	Listeners []Listener
}

type QueueExsistsError struct {
}

func (e *QueueExsistsError) Error() string {
	return "Queue Already Exsists"
}

type QueueDoesNotExsists struct {
}

func (e QueueDoesNotExsists) Error() string {
	return "Queue does not exsists"
}

func NewQueue(name string) *Queue {
	return &Queue{
		Name: name,
		ind:  0,
	}
}

// func ListAllQueues() {
// 	ds := NewDataStorage()
// }

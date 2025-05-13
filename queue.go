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

func ListAllQueues() ([]string, error) {
	ds := NewDataStorage()
	arr := make([]string, 0)
	qs, err := ds.GetQueues()
	if err != nil {
		return arr, err
	}
	for _, q := range qs {
		arr = append(arr, string(q))
	}
	return arr, nil
}

func DeleteQueue(qname string) error {
	ds := NewDataStorage()
	return ds.DeleteQueue(qname)
}

func CreateQueue(qname string) error {
	ds := NewDataStorage()
	return ds.CreateQueue(qname)
}

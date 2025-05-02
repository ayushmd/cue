package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouterPattern(t *testing.T) {
	r := &Router{
		queues: make(map[string]*Queue),
	}
	testcases := []string{"queue.1", "queue.2", "queue.3"}
	for _, v := range testcases {
		r.queues[v] = NewQueue(v)
	}
	arr := r.GetMatchingQueues("queue.*")
	fmt.Println(arr[0].Name, arr[1].Name, arr[2].Name)
	assert.Len(t, arr, 3)
}

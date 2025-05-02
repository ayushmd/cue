package main

import "regexp"

type Router struct {
	queues map[string]*Queue
}

func (r *Router) GetMatchingQueues(pattern string) []*Queue {
	arr := make([]*Queue, 0)
	for k, v := range r.queues {
		if k == pattern {
			arr = append(arr, v)
		} else if re := regexp.MustCompile(pattern); re.MatchString(k) {
			arr = append(arr, v)
		}
	}
	return arr
}

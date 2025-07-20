# DelayedQ

```
go get github.com/ayushmd/cue@latest
```

Recieve messages from queue after a certain time or on a certain time. Useful for scheduling jobs, notifications.

```go
package cue

import (
	"fmt"
	"time"

	"github.com/ayushmd/cue"
)

type TTLItem struct {
	id        int
	createdAt int64
}

func main() {
	ttlq := delayedQ.NewQueue()

	// Background queue listner
	go func() {
		for {
			select {
			case job := <-ttlq.Subscribe():
				jobj := job.(*TTLItem)
				fmt.Printf(
					"Recieved Job %d: Created At: %d Recieved At: %d\n",
					jobj.id, jobj.createdAt, time.Now().Unix(),
				)
			}
		}
	}()

	ttlq.Push(&TTLItem{
		id:        1,
		createdAt: time.Now().Unix(),
	}, time.Now().Add(10*time.Second).Unix())

	ttlq.Push(&TTLItem{
		id:        2,
		createdAt: time.Now().Unix(),
	}, time.Now().Add(5*time.Second).Unix())

	ttlq.Push(&TTLItem{
		id:        3,
		createdAt: time.Now().Unix(),
	}, time.Now().Add(2*time.Second).Unix())
	select {}
}
```

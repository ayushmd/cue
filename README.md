# Cue

Recieve messages from queue after a certain time or on a certain time. Useful for scheduling jobs, notification systems, expiring records with callback. 

## Setting Up Server

### Using Docker

Below is the pre-built docker image for cue-server

```
docker pull cuekit/cue-server:latest
```

Run Using:
```
docker run --name cue-server -d -p 6336:6336 cue-server
```

**Note:** Port **6336** is the default port on which cue-server runs on

## Using Cue with Go Cue-Client

Install library with below command

```
go get github.com/ayushmd/cue@latest
```

## Examples
### Listen to queue

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ayushmd/cue/pkg/cuecl"
)

func main() {
	cli, err := cuecl.NewCueClient(":6336")
	if err != nil {
		log.Fatal("Failed to connect to server")
	}
	defer cli.Close()

	var queueName string = "test"

	err = cli.CreateQueue(queueName)
	if err != nil {
		log.Fatal("Failed to create queue")
	}

	ch, err := cli.Listen(queueName)
	if err != nil {
		log.Fatal("Failed to create Listen")
	}

	for data := range ch {
		fmt.Println("Recieved: ", string(data), time.Now().UnixMilli())
	}
}
```

### Adding a item to queue

```go
package main

import (
	"fmt"
	"time"
	"log"

	"github.com/ayushmd/cue/pkg/cuecl"
)

func main() {
	cli, err := cuecl.NewCueClient(":6336")
	if err != nil {
		log.Fatal("Failed to connect to server")
	}
	defer cli.Close()

	ttl := time.Now().Add(10 * time.Second).UnixMilli()

	message := []byte(fmt.Sprintf("{'data':'test data', 'createdAt': '%d'}", time.Now().UnixMilli()))

	queueName := "test"

	err = cli.PushItem(queueName, message, ttl)
	if err != nil {
		fmt.Println("Failed to create item ", err)
	}

}
```

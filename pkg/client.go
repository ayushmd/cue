package pkg

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

type SchedulerClient struct {
	baseURL string
	client  *http.Client
}

func NewSchedulerClient(serverURL string) *SchedulerClient {
	// transport := &http2.Transport{
	// 	// InsecureSkipVerify only for local dev/self-signed certs!
	// 	AllowHTTP: false,
	// }
	return &SchedulerClient{
		baseURL: serverURL,
		client: &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		},
	}
}

func (sc *SchedulerClient) ListenQueue(queueName string, callback func(message string)) {
	url := fmt.Sprintf("%s/item/listen/%s", sc.baseURL, queueName)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println("Failed to create request:", err)
		return
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			req, _ := http.NewRequest("GET", sc.baseURL, nil)
			req.Header.Set("X-Ping", "keepturn")
			_, err := sc.client.Do(req)
			if err != nil {
				log.Println("Ping failed:", err)
			} else {
				log.Println("Ping successful")
			}
		}
	}()

	resp, err := sc.client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println("Response status:", resp.Status)

	// Read the response stream line by line
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("Received:", string(line))
	}

	// Handle error if the scanner exits unexpectedly
	if err := scanner.Err(); err != nil {
		fmt.Println("Scanner error:", err)
	} else {
		fmt.Println("Stream closed by server.")
	}
}

func (sc *SchedulerClient) Send(body interface{}) int {
	return sc.sendRequestWithBody("/item/push", body)
}

func (sc *SchedulerClient) InitQueue(queueName string) int {
	return sc.sendRequestWithBody("/queue/create", map[string]string{"queueName": queueName})
}

func (sc *SchedulerClient) DeleteQueue(queueName string) int {
	return sc.sendRequestWithBody("/queue/delete", map[string]string{"queueName": queueName})
}

func (sc *SchedulerClient) ListQueues() {
	url := sc.baseURL + "/queue/list"
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := sc.client.Do(req)
	if err != nil {
		fmt.Println("ListQueues error:", err)
		return
	}
	defer resp.Body.Close()

	var res struct {
		Data    []string `json:"data"`
		Success bool     `json:"success"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Println("Failed to parse list response:", err)
		return
	}

	if !res.Success {
		log.Println("ListQueues: server reported failure")
		return
	}

	// fmt.Println("Queues:")
	for _, q := range res.Data {
		fmt.Println(q)
	}
}

func (sc *SchedulerClient) ack(id int) int {
	path := fmt.Sprintf("/item/ack/%d", id)
	return sc.sendRequestWithBody(path, map[string]bool{"acked": true})
}

func (sc *SchedulerClient) sendRequestWithBody(path string, body interface{}) int {
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		log.Println("Failed to marshal body:", err)
		return 0
	}

	url := sc.baseURL + path
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBytes))
	if err != nil {
		log.Println("Request creation failed:", err)
		return 0
	}

	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(jsonBytes))

	resp, err := sc.client.Do(req)
	if err != nil {
		log.Println("POST failed:", err)
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode

	// data, _ := io.ReadAll(resp.Body)
	// log.Println("Response:", string(data))
}

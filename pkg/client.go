package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

type SchedulerClient struct {
	baseURL string
	client  *http.Client
}

func NewSchedulerClient(serverURL string) *SchedulerClient {
	transport := &http2.Transport{
		// InsecureSkipVerify only for local dev/self-signed certs!
		AllowHTTP: false,
	}
	return &SchedulerClient{
		baseURL: serverURL,
		client: &http.Client{
			Transport: transport,
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
			resp, err := sc.client.Do(req)
			if err != nil {
				log.Println("Error in listen request:", err)
				time.Sleep(5 * time.Second)
				continue
			}

			reader := resp.Body
			defer reader.Close()

			decoder := json.NewDecoder(reader)
			for {
				var raw json.RawMessage
				if err := decoder.Decode(&raw); err != nil {
					if err == io.EOF {
						log.Println("Stream closed by server.")
					} else {
						log.Println("Decode error:", err)
					}
					break
				}

				var msg map[string]interface{}
				if err := json.Unmarshal(raw, &msg); err != nil {
					log.Println("Failed to unmarshal:", err)
					continue
				}

				if id, ok := msg["id"].(float64); ok {
					sc.ack(int(id))
				}

				if data, ok := msg["data"].(string); ok {
					callback(data)
				}
			}
		}
	}()

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
}

func (sc *SchedulerClient) Send(body interface{}) {
	sc.sendRequestWithBody("/item/push", body)
}

func (sc *SchedulerClient) InitQueue(queueName string) {
	sc.sendRequestWithBody("/queue/create", map[string]string{"queueName": queueName})
}

func (sc *SchedulerClient) DeleteQueue(queueName string) {
	sc.sendRequestWithBody("/queue/delete", map[string]string{"queueName": queueName})
}

func (sc *SchedulerClient) ListQueues() {
	url := sc.baseURL + "/queue/list"
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := sc.client.Do(req)
	if err != nil {
		log.Println("ListQueues error:", err)
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

	log.Println("Queues:")
	for _, q := range res.Data {
		log.Println(q)
	}
}

func (sc *SchedulerClient) ack(id int) {
	path := fmt.Sprintf("/item/ack/%d", id)
	sc.sendRequestWithBody(path, map[string]bool{"acked": true})
}

func (sc *SchedulerClient) sendRequestWithBody(path string, body interface{}) {
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		log.Println("Failed to marshal body:", err)
		return
	}

	url := sc.baseURL + path
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBytes))
	if err != nil {
		log.Println("Request creation failed:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(jsonBytes))

	resp, err := sc.client.Do(req)
	if err != nil {
		log.Println("POST failed:", err)
		return
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	log.Println("Response:", string(data))
}

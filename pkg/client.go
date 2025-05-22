package pkg

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/ayushmd/delayedQ/rpc"
	"google.golang.org/grpc"
)

type SchedulerClient struct {
	conn   *grpc.ClientConn
	client pb.SchedulerServiceClient
}

func NewSchedulerClient(addr string) (*SchedulerClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure()) // Use credentials for production
	if err != nil {
		return nil, err
	}

	return &SchedulerClient{
		conn:   conn,
		client: pb.NewSchedulerServiceClient(conn),
	}, nil
}

func (sc *SchedulerClient) Close() error {
	return sc.conn.Close()
}

func (sc *SchedulerClient) CreateQueue(name string) error {
	_, err := sc.client.CreateQueue(context.Background(), &pb.QueueNameRequest{QueueName: name})
	if err != nil {
		return err
	}
	return nil
}

func (sc *SchedulerClient) PushItem(queueName string, data []byte, ttl int64) error {
	res, err := sc.client.PushItem(context.Background(), &pb.ItemRequest{
		QueueName: queueName,
		Data:      data,
		Ttl:       ttl,
	})
	if err != nil {
		return err
	}
	fmt.Println("PushItem success:", res.Success)
	return nil
}

func (sc *SchedulerClient) Listen(queueName string, handle func(data []byte)) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := sc.client.Listen(ctx, &pb.QueueNameRequest{QueueName: queueName})
	if err != nil {
		return err
	}

	fmt.Println("Listening for items...")
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		fmt.Printf("Received Item: ID=%d, Data=%s, Success=%v\n", item.Id, string(item.Data), item.Success)
		handle(item.Data)

		// Ack after handling
		if err := sc.Ack(item.Id); err != nil {
			log.Printf("Ack error: %v", err)
		}
	}
	return nil
}

func (sc *SchedulerClient) Ack(id int64) error {
	res, err := sc.client.Ack(context.Background(), &pb.AckRequest{Id: id})
	if err != nil {
		return err
	}
	fmt.Printf("Ack ID=%d, success=%v\n", id, res.Success)
	return nil
}

func (sc *SchedulerClient) ListQueues() ([]string, error) {
	res, err := sc.client.ListQueues(context.Background(), &pb.Empty{})
	if err != nil {
		return nil, err
	}
	// fmt.Println("Queues:", res.Data, "Success:", res.Success)
	return res.Data, nil
}

func (sc *SchedulerClient) DeleteQueue(name string) error {
	_, err := sc.client.DeleteQueue(context.Background(), &pb.QueueNameRequest{QueueName: name})
	if err != nil {
		return err
	}
	return nil
}

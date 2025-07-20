package cuecl

import (
	"context"
	"io"

	pb "github.com/ayushmd/cue/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CueClient struct {
	conn   *grpc.ClientConn
	client pb.SchedulerServiceClient
}

func NewCueClient(addr string) (*CueClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &CueClient{
		conn:   conn,
		client: pb.NewSchedulerServiceClient(conn),
	}, nil
}

func (sc *CueClient) Ping() (bool, error) {
	res, err := sc.client.Ping(context.Background(), &pb.Empty{})
	return res.GetSuccess(), err
}

func (sc *CueClient) Close() error {
	return sc.conn.Close()
}

func (sc *CueClient) CreateQueue(name string) error {
	_, err := sc.client.CreateQueue(context.Background(), &pb.QueueNameRequest{QueueName: name})
	if err != nil {
		return err
	}
	return nil
}

func (sc *CueClient) PushItem(queueName string, data []byte, ttl int64) error {
	_, err := sc.client.PushItem(context.Background(), &pb.ItemRequest{
		QueueName: queueName,
		Data:      data,
		Ttl:       ttl,
	})
	if err != nil {
		return err
	}
	return nil
}

func (sc *CueClient) Listen(queueName string) (chan []byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	stream, err := sc.client.Listen(ctx, &pb.QueueNameRequest{QueueName: queueName})
	if err != nil {
		cancel()
		return nil, err
	}

	ch := make(chan []byte, 1000)
	ackch := make(chan int64, 1000)

	go func() {
		for ackid := range ackch {
			sc.Ack(ackid)
		}
	}()

	// fmt.Println("Listening for items...")
	go func() {
		defer func() {
			cancel()
			close(ch)
			close(ackch)
		}()
		for {
			item, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return
			}
			ch <- item.Data
			if item.Ack {
				ackch <- item.Id
			}
		}
	}()
	return ch, nil
}

func (sc *CueClient) Ack(id int64) error {
	_, err := sc.client.Ack(context.Background(), &pb.AckRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

func (sc *CueClient) ListQueues() ([]string, error) {
	res, err := sc.client.ListQueues(context.Background(), &pb.Empty{})
	if err != nil {
		return nil, err
	}
	// fmt.Println("Queues:", res.Data, "Success:", res.Success)
	return res.Data, nil
}

func (sc *CueClient) DeleteQueue(name string) error {
	_, err := sc.client.DeleteQueue(context.Background(), &pb.QueueNameRequest{QueueName: name})
	if err != nil {
		return err
	}
	return nil
}

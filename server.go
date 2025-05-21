package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"

	pb "github.com/ayushmd/delayedQ/rpc"
)

type Server struct {
	pb.UnimplementedSchedulerServiceServer
	m  *Scheduler
	mu sync.Mutex
	// srv    *http.Server
	grpcs  *grpc.Server
	lis    net.Listener
	nextID int
}

func NewServer() *Server {
	s := &Server{
		m:      NewScheduler(),
		nextID: 0,
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcs := grpc.NewServer()
	pb.RegisterSchedulerServiceServer(grpcs, s)
	log.Printf("server listening at %v", lis.Addr())
	s.grpcs = grpcs
	s.lis = lis
	// mux := s.RegisterRoutes()
	// h2s := &http2.Server{}
	// server := &http.Server{
	// 	Addr:    ":8080",
	// 	Handler: h2c.NewHandler(mux, h2s),
	// }
	// s.srv = server
	return s
}

func (s *Server) Start() error {
	return s.grpcs.Serve(s.lis)
}

func (s *Server) GetNextID() int {
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()
	return id
}

func (s *Server) Listen(req *pb.QueueNameRequest, stream pb.SchedulerService_ListenServer) error {
	if !s.m.r.CheckExsists(req.QueueName) {
		return fmt.Errorf("queue not found")
	}

	id := s.GetNextID()
	listener := Listener{
		id: id,
		send: func(id int64, data []byte) error {
			if len(data) > 20 {
				fmt.Println("sender ", string(data[:20]))
			} else {
				fmt.Println("sender ", data)
			}
			return stream.Send(&pb.ItemResponse{
				Id:      id,
				Data:    data,
				Success: true,
			})
		},
	}

	s.m.r.AddListener(req.QueueName, listener)

	<-stream.Context().Done()

	s.m.r.RemoveListener(req.QueueName, id)
	return nil
}

func (s *Server) PushItem(ctx context.Context, in *pb.ItemRequest) (*pb.Response, error) {
	item := Item{
		QueueName: in.GetQueueName(),
		TTL:       in.GetTtl(),
		Data:      in.GetData(),
	}
	err := s.m.CreateItem(item)
	return &pb.Response{Success: err == nil}, err
}

func (s *Server) Ack(ctx context.Context, in *pb.AckRequest) (*pb.Response, error) {
	s.m.Ack(in.GetId())
	return &pb.Response{Success: true}, nil
}

func (s *Server) CreateQueue(ctx context.Context, in *pb.QueueNameRequest) (*pb.Response, error) {
	err := s.m.CreateQueue(in.QueueName)
	return &pb.Response{Success: err == nil}, err
}

func (s *Server) DeleteQueue(ctx context.Context, in *pb.QueueNameRequest) (*pb.Response, error) {
	err := s.m.DeleteQueue(in.QueueName)
	return &pb.Response{Success: err == nil}, err
}

func (s *Server) ListQueues(ctx context.Context, _ *pb.Empty) (*pb.ListQueueResponse, error) {
	qs := s.m.ListQueues()
	return &pb.ListQueueResponse{
		Data:    qs,
		Success: true,
	}, nil
}

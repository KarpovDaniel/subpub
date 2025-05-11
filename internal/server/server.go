package server

import (
	"context"
	"io"
	"log"

	"subpub/internal/subpub"
	pb "subpub/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	pb.UnimplementedPubSubServer
	bus subpub.SubPub
}

func NewService(bus subpub.SubPub) *Service {
	return &Service{bus: bus}
}

func (s *Service) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if req.Data == "" {
		return nil, status.Error(codes.InvalidArgument, "empty data")
	}

	log.Printf("[Publish] key=%q data=%q", req.Key, req.Data)
	if err := s.bus.Publish(req.Key, req.Data); err != nil {
		log.Printf("[Publish] error publishing: %v", err)
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "empty key")
	}

	log.Printf("[Subscribe] new subscription for key=%q", req.Key)
	msgCh := make(chan string, 16)

	sub, err := s.bus.Subscribe(req.Key, func(msg interface{}) {
		str, ok := msg.(string)
		if !ok {
			log.Printf("[Subscribe] skip non-string message: %T", msg)
			return
		}
		select {
		case msgCh <- str:
			log.Printf("[Subscribe] queued message %q for client", str)
		case <-stream.Context().Done():
			log.Printf("[Subscribe] client context done, dropping %q", str)
		}
	})
	if err != nil {
		log.Printf("[Subscribe] subscribe error: %v", err)
		return status.Errorf(codes.Internal, "subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-stream.Context().Done():
			log.Printf("[Subscribe] client canceled subscription for key=%q", req.Key)
			return status.Error(codes.Canceled, "client cancelled")
		case data := <-msgCh:
			log.Printf("[Subscribe] sending %q to client", data)
			if err := stream.Send(&pb.Event{Data: data}); err != nil {
				if err == io.EOF {
					return nil
				}
				log.Printf("[Subscribe] send error: %v", err)
				return status.Errorf(codes.Unknown, "send error: %v", err)
			}
		}
	}
}

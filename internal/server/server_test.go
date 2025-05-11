package server

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"subpub/internal/subpub"
	pb "subpub/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func startTestServer(t *testing.T) (pb.PubSubClient, func()) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	grpcServer := grpc.NewServer()
	bus := subpub.NewSubPub()
	svc := NewService(bus)
	pb.RegisterPubSubServer(grpcServer, svc)
	go grpcServer.Serve(lis)

	_, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		t.Fatalf("NewClient failed: %v", err)
	}
	conn.Connect()
	cancel()

	return pb.NewPubSubClient(conn), func() {
		conn.Close()
		grpcServer.GracefulStop()
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	client, shutdown := startTestServer(t)
	defer shutdown()

	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Key: "topic"})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	msgCh := make(chan *pb.Event, 1)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)

	go func() {
		msg, err := stream.Recv()
		if err != nil {
			errCh <- err
		} else {
			msgCh <- msg
		}
		close(doneCh)
	}()

	time.Sleep(10 * time.Millisecond)

	if _, err := client.Publish(context.Background(), &pb.PublishRequest{Key: "topic", Data: "hello"}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case msg := <-msgCh:
		if msg.Data != "hello" {
			t.Errorf("got %q, want %q", msg.Data, "hello")
		}
	case err := <-errCh:
		t.Fatalf("Recv error: %v", err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for message")
	}

	<-doneCh
}

func TestMultipleSubscribers(t *testing.T) {
	client, shutdown := startTestServer(t)
	defer shutdown()

	s1, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Subscribe1 failed: %v", err)
	}
	s2, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Subscribe2 failed: %v", err)
	}

	type recvResult struct {
		data string
		err  error
	}
	ch1 := make(chan recvResult, 1)
	ch2 := make(chan recvResult, 1)
	doneCh := make(chan struct{}, 2)

	go func() {
		msg, err := s1.Recv()
		if err != nil {
			ch1 <- recvResult{"", err}
		} else {
			ch1 <- recvResult{msg.Data, nil}
		}
		doneCh <- struct{}{}
	}()
	go func() {
		msg, err := s2.Recv()
		if err != nil {
			ch2 <- recvResult{"", err}
		} else {
			ch2 <- recvResult{msg.Data, nil}
		}
		doneCh <- struct{}{}
	}()

	time.Sleep(10 * time.Millisecond)

	if _, err := client.Publish(context.Background(), &pb.PublishRequest{Key: "k", Data: "msg"}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	timeout := time.After(500 * time.Millisecond)
	for i, ch := range []chan recvResult{ch1, ch2} {
		select {
		case res := <-ch:
			if res.err != nil {
				t.Fatalf("subscriber %d Recv error: %v", i+1, res.err)
			}
			if res.data != "msg" {
				t.Errorf("subscriber %d got %q, want %q", i+1, res.data, "msg")
			}
		case <-timeout:
			t.Fatalf("timeout waiting for subscriber %d", i+1)
		}
	}

	<-doneCh
	<-doneCh
}

func TestSubscribeEmptyKey(t *testing.T) {
	client, shutdown := startTestServer(t)
	defer shutdown()

	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Key: ""})
	if err != nil {
		t.Fatalf("Subscribe failed unexpectedly: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for empty key on Recv()")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("got code %v, want InvalidArgument", st.Code())
	}
}

func TestPublishInvalidInput(t *testing.T) {
	client, shutdown := startTestServer(t)
	defer shutdown()

	cases := []*pb.PublishRequest{
		{Key: "", Data: "d"},
		{Key: "k", Data: ""},
	}
	for _, req := range cases {
		_, err := client.Publish(context.Background(), req)
		if err == nil {
			t.Errorf("expected error for %+v", req)
			continue
		}
		if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
			t.Errorf("got code %v, want InvalidArgument", st.Code())
		}
	}
}

func TestSubscribeCancel(t *testing.T) {
	client, shutdown := startTestServer(t)
	defer shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Key: "abc"})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	cancel()

	msg, err := stream.Recv()
	if msg != nil {
		t.Errorf("expected no msg, got %v", msg)
	}
	if err != io.EOF && status.Code(err) != codes.Canceled {
		t.Errorf("got error %v, want EOF or Canceled", err)
	}
}

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"subpub/internal/config"
	"subpub/internal/server"
	"subpub/internal/subpub"
	pb "subpub/proto"

	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("gRPC server listening on :%d", cfg.GRPCPort)

	// создаём шину событий
	bus := subpub.NewSubPub()
	// создаём сервис
	svc := server.NewService(bus)

	// запускаем gRPC
	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServer(grpcServer, svc)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC Serve error: %v", err)
		}
	}()

	// graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	log.Println("shutdown signal received")

	grpcServer.GracefulStop()

	// ждём закрытия шины
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err = bus.Close(ctx); err != nil {
		log.Printf("bus.Close timeout: %v", err)
	}
	log.Println("server stopped")
}

package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"subpub/internal/server"
	"subpub/internal/subpub"
	pb "subpub/proto"

	"google.golang.org/grpc"
)

func main() {
	// загрузка .env (если есть)
	_ = godotenv.Load()

	// читаем из окружения
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}
	timeoutStr := os.Getenv("SHUTDOWN_TIMEOUT")
	if timeoutStr == "" {
		timeoutStr = "5s"
	}
	shutdownTimeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		log.Fatalf("invalid SHUTDOWN_TIMEOUT: %v", err)
	}

	addr := fmt.Sprintf(":%s", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s failed: %v", addr, err)
	}
	log.Printf("gRPC server listening on %s", addr)

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
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := bus.Close(ctx); err != nil {
		log.Printf("bus.Close timeout: %v", err)
	}
	log.Println("server stopped")
}

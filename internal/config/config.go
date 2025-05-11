package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	GRPCPort        int
	ShutdownTimeout time.Duration
}

func Load(path string) (*Config, error) {
	_ = godotenv.Load(path)

	portStr := os.Getenv("GRPC_PORT")
	timeoutStr := os.Getenv("SHUTDOWN_TIMEOUT")

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, err
	}

	return &Config{
		GRPCPort:        port,
		ShutdownTimeout: timeout,
	}, nil
}

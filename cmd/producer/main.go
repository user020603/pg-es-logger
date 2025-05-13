package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"thanhnt208/pg-cdc-es/configs"
	"thanhnt208/pg-cdc-es/internal/repositories"
	"thanhnt208/pg-cdc-es/internal/services"
	"thanhnt208/pg-cdc-es/pkg/kafka"
	"thanhnt208/pg-cdc-es/pkg/logger"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load("../.env")

	logger, err := logger.NewLogger("info")
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	pgConn, err := configs.ConnectPostgres()
	if err != nil {
		logger.Error("failed to connect to Postgres", "error", err)
		return
	}

	pgAuditRepo := repositories.NewAuditPostgresRepository(pgConn)

	kafkaWriterRaw, err := configs.ConnectKafkaWriter()
	if err != nil {
		logger.Error("failed to connect to Kafka", "error", err)
		return
	}
	defer kafkaWriterRaw.Close()
	kafkaWriter := kafka.NewKafkaWriter(kafkaWriterRaw)

	batchSize := getEnvAsInt("BATCH_SIZE", 1000)
	numWorkers := getEnvAsInt("NUM_WORKERS", 5)

	producerService := services.NewSyncProducerService(
		pgAuditRepo,
		kafkaWriter,
		batchSize,
		numWorkers,
		logger,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	logger.Info("Starting Kafka producer service...")
	if err := producerService.Start(ctx); err != nil {
		logger.Error("Producer service stopped with error", "error", err)
		return
	}
	logger.Info("Producer service stopped")
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

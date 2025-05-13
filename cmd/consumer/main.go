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

	kafkaReaderRaw, err := configs.ConnectKafkaReader()
	if err != nil {
		logger.Error("failed to connect to Kafka", "error", err)
		return
	}
	defer kafkaReaderRaw.Close()
	kafkaReader := kafka.NewKafkaReader(kafkaReaderRaw)

	esClient, err := configs.ConnectElasticsearch()
	if err != nil {
		logger.Error("failed to connect to Elasticsearch", "error", err)
		return
	}

	esIndex := getEnv("ES_INDEX", "pg_audit_logs")
	esAuditRepo, err := repositories.NewAuditESRepository(esClient, esIndex, logger)
	if err != nil {
		logger.Fatal("Failed to create Elasticsearch repository: %v", err)
	}

	numWorkers := getEnvAsInt("NUM_WORKERS", 5)

	consumerService := services.NewSyncConsumerService(
		kafkaReader,
		esAuditRepo,
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

	logger.Info("Starting Kafka consumer service...")
	if err := consumerService.Start(ctx); err != nil {
		logger.Error("Consumer service stopped with error", "error", err)
		return
	}
	logger.Info("Consumer service stopped")
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

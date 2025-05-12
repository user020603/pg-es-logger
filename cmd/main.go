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

	batchSize := getEnvAsInt("BATCH_SIZE", 1000)
	numWorkers := getEnvAsInt("NUM_WORKERS", 5)

	syncService := services.NewSyncService(
		pgAuditRepo,
		esAuditRepo,
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

	logger.Info("Starting sync service...")
	if err := syncService.Start(ctx); err != nil {
		logger.Error("Failed to start sync service", "error", err)
		return
	}
	logger.Info("Sync service stopped")
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

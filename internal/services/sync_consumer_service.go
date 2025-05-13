package services

import (
	"context"
	"encoding/json"
	"thanhnt208/pg-cdc-es/internal/models"
	"thanhnt208/pg-cdc-es/internal/repositories"
	"thanhnt208/pg-cdc-es/pkg/kafka"
	"thanhnt208/pg-cdc-es/pkg/logger"
	"time"
)

type ISyncConsumerService interface {
	Start(ctx context.Context) error
}

type SyncConsumerService struct {
	kafkaReader kafka.IKafkaReader
	auditEsRepo repositories.IAuditESRepository
	numWorkers  int
	logger      logger.ILogger
}

var _ ISyncConsumerService = (*SyncConsumerService)(nil)

func NewSyncConsumerService(
	kafkaReader kafka.IKafkaReader,
	auditEsRepo repositories.IAuditESRepository,
	numWorkers int,
	logger logger.ILogger,
) ISyncConsumerService {
	return &SyncConsumerService{
		kafkaReader: kafkaReader,
		auditEsRepo: auditEsRepo,
		numWorkers:  numWorkers,
		logger:      logger,
	}
}

func (s *SyncConsumerService) Start(ctx context.Context) error {
	msgs := make(chan []byte, s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		go s.worker(ctx, msgs)
	}

	for {
		select {
		case <-ctx.Done():
			close(msgs)
			return ctx.Err()
		default:
			msg, err := s.kafkaReader.Read(ctx)
			if err != nil {
				s.logger.Error("Failed to read from Kafka", "error", err)
				time.Sleep(time.Second)
				continue
			}
			msgs <- msg
		}
	}
}

func (s *SyncConsumerService) worker(ctx context.Context, msgs <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("consumer worker shutting down")
			return
		case msg, ok := <-msgs:
			if !ok {
				s.logger.Info("no more messages to process, shutting down worker")
				return
			}

			var logs []models.ElasticAuditLog
			if err := json.Unmarshal(msg, &logs); err != nil {
				s.logger.Error("Failed to unmarshal batch from Kafka", "error", err)
				continue
			}
			if len(logs) == 0 {
				s.logger.Info("Received empty batch from Kafka")
				continue
			}

			if err := s.auditEsRepo.BulkIndexLogs(ctx, logs); err != nil {
				s.logger.Error("Bulk indexing logs to ES failed", "error", err)
			} else {
				s.logger.Info("Successfully bulk indexed logs to ES", "count", len(logs))
			}
		}
	}
}

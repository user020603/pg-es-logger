package services

import (
	"context"
	"encoding/json"
	"sync"
	"thanhnt208/pg-cdc-es/internal/models"
	"thanhnt208/pg-cdc-es/internal/repositories"
	"thanhnt208/pg-cdc-es/pkg/kafka"
	"thanhnt208/pg-cdc-es/pkg/logger"
	"time"

	"github.com/jmoiron/sqlx"
)

type jobPayloadProducer struct {
	logs []models.AuditLog
	tx   *sqlx.Tx
}

type ISyncProducerService interface {
	Start(ctx context.Context) error
}

type SyncProducerService struct {
	auditPgRepo repositories.IAuditPostgresRepository
	kafkaWriter kafka.IKafkaWriter
	batchSize   int
	numWorkers  int
	pollTimeout time.Duration
	logger      logger.ILogger
}

var _ ISyncProducerService = (*SyncProducerService)(nil)

func NewSyncProducerService(
	auditPgRepo repositories.IAuditPostgresRepository,
	kafkaWriter kafka.IKafkaWriter,
	batchSize, numWorkers int,
	logger logger.ILogger,
) ISyncProducerService {
	return &SyncProducerService{
		auditPgRepo: auditPgRepo,
		kafkaWriter: kafkaWriter,
		batchSize:   batchSize,
		numWorkers:  numWorkers,
		pollTimeout: 5 * time.Second,
		logger:      logger,
	}
}

func (s *SyncProducerService) Start(ctx context.Context) error {
	jobs := make(chan jobPayloadProducer, s.numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < s.numWorkers; i++ {
		wg.Add(1)
		go s.worker(ctx, jobs, &wg)
	}

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		default:
			logs, tx, err := s.auditPgRepo.GetUnprocessedLogs(ctx, s.batchSize)
			if err != nil {
				s.logger.Error("failed to get unprocessed logs", "error", err)
				time.Sleep(time.Second)
				continue
			}

			if len(logs) == 0 {
				if tx != nil {
					_ = tx.Rollback()
				}
				time.Sleep(s.pollTimeout)
				continue
			}

			s.logger.Info("fetched logs for Kafka publishing", "count", len(logs))
			jobs <- jobPayloadProducer{
				logs: logs,
				tx:   tx,
			}
		}
	}
}

func (s *SyncProducerService) worker(ctx context.Context, jobs <-chan jobPayloadProducer, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("worker shutting down")
			return
		case job, ok := <-jobs:
			if !ok {
				s.logger.Info("no more jobs to process")
				return
			}
			if err := s.publishLogs(ctx, job.logs, job.tx); err != nil {
				s.logger.Error("failed to publish logs", "error", err)
			}
		}
	}
}

func (s *SyncProducerService) publishLogs(ctx context.Context, logs []models.AuditLog, tx *sqlx.Tx) error {
	if len(logs) == 0 {
		return nil
	}

	batch, err := json.Marshal(logs)
	if err != nil {
		_ = tx.Rollback()
		s.logger.Error("failed to marshal logs for Kafka", "error", err)
	}

	if err := s.kafkaWriter.Send(ctx, batch); err != nil {
		_ = tx.Rollback()
		s.logger.Error("failed to send log batch to Kafka", "error", err)
		return err
	}

	ids := make([]int64, len(logs))
	for i, log := range logs {
		ids[i] = log.ID
	}

	if err := s.auditPgRepo.MarkLogsProcessed(tx, ids); err != nil {
		_ = tx.Rollback()
		s.logger.Error("marking logs as processed failed", "error", err)
		return err
	}

	if err := tx.Commit(); err != nil {
		s.logger.Error("committing transaction failed", "error", err)
		return err
	}

	s.logger.Info("published logs to Kafka and commited", "count", len(logs))
	return nil
}

package services

import (
	"context"
	"encoding/json"
	"sync"
	"thanhnt208/pg-cdc-es/internal/models"
	"thanhnt208/pg-cdc-es/internal/repositories"
	"thanhnt208/pg-cdc-es/pkg/logger"
	"time"

	"github.com/jmoiron/sqlx"
)

type jobPayload struct {
	logs []models.AuditLog
	tx   *sqlx.Tx
}

type ISyncService interface {
	Start(ctx context.Context) error
}

type SyncService struct {
	auditPgRepo repositories.IAuditPostgresRepository
	auditEsRepo repositories.IAuditESRepository
	batchSize   int
	numWorkers  int
	pollTimeout time.Duration
	logger      logger.ILogger
}

var _ ISyncService = (*SyncService)(nil)

func NewSyncService(
	auditPgRepo repositories.IAuditPostgresRepository,
	auditEsRepo repositories.IAuditESRepository,
	batchSize, numWorkers int,
	logger logger.ILogger,
) ISyncService {
	return &SyncService{
		auditPgRepo: auditPgRepo,
		auditEsRepo: auditEsRepo,
		batchSize:   batchSize,
		numWorkers:  numWorkers,
		pollTimeout: 5 * time.Second,
		logger:      logger,
	}
}

func (s *SyncService) Start(ctx context.Context) error {
	jobs := make(chan jobPayload, s.numWorkers)
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
				s.logger.Error("Failed to get unprocessed logs", "error", err)
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

			s.logger.Info("Fetched logs for processing", "count", len(logs))
			jobs <- jobPayload{
				logs: logs,
				tx:   tx,
			}
		}
	}
}

func (s *SyncService) worker(ctx context.Context, jobs <-chan jobPayload, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("worker shutting down")
		case job, ok := <-jobs:
			if !ok {
				s.logger.Info("no more jobs to process")
				return
			}
			if err := s.processLogs(ctx, job.logs, job.tx); err != nil {
				s.logger.Error("failed to process logs", "error", err)
			}
		}
	}
}

func (s *SyncService) processLogs(ctx context.Context, logs []models.AuditLog, tx *sqlx.Tx) error {
	if len(logs) == 0 {
		return nil
	}

	esAuditLogs := make([]models.ElasticAuditLog, len(logs))
	ids := make([]int64, len(logs))

	for i, log := range logs {
		esAuditLogs[i] = models.ElasticAuditLog{
			TableName: log.TableName,
			Operation: log.Operation,
			Timestamp: log.CreatedAt,
		}

		if log.OldData.Valid && log.OldData.String != "" {
			esAuditLogs[i].OldData = json.RawMessage(log.OldData.String)
		}

		if log.NewData.Valid && log.NewData.String != "" {
			esAuditLogs[i].NewData = json.RawMessage(log.NewData.String)
		}

		ids[i] = log.ID
	}

	if err := s.auditEsRepo.BulkIndexLogs(ctx, esAuditLogs); err != nil {
		_ = tx.Rollback()
		s.logger.Error("Bulk indexing logs failed. Transaction rolled back.", "error", err)
		return err
	}

	if err := s.auditPgRepo.MarkLogsProcessed(tx, ids); err != nil {
		_ = tx.Rollback()
		s.logger.Error("Marking logs as processed failed. Transaction rolled back.", "error", err)
		return err
	}

	if err := tx.Commit(); err != nil {
		s.logger.Error("Committing transaction failed", "error", err)
		return err
	}

	s.logger.Info("Successfully processed and committed logs", "count", len(logs))
	return nil
}

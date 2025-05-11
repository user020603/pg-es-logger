package repositories

import (
	"context"
	"database/sql"
	"thanhnt208/pg-cdc-es/internal/models"
	"time"

	"github.com/jmoiron/sqlx"
)

type IAuditPostgresRepository interface {
	GetUnprocessedLogs(ctx context.Context, limit int) ([]models.AuditLog, *sqlx.Tx, error)
	MarkLogsProcessed(tx *sqlx.Tx, ids []int64) error
	ResetFailedLogs(ctx context.Context, timeout time.Duration) error
}

type AuditPostgresRepository struct {
	db *sqlx.DB
}

var _ IAuditPostgresRepository = (*AuditPostgresRepository)(nil)

func NewAuditPostgresRepository(db *sqlx.DB) IAuditPostgresRepository {
	return &AuditPostgresRepository{
		db: db,
	}
}

func (r *AuditPostgresRepository) GetUnprocessedLogs(ctx context.Context, limit int) ([]models.AuditLog, *sqlx.Tx, error) {
	logs := []models.AuditLog{}
	tx, err := r.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, nil, err
	}

	query := `
		SELECT id, table_name, operation, old_data, new_data, created_at, processed
		FROM auditdb
		WHERE processed = FALSE
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`

	err = tx.SelectContext(ctx, &logs, query, limit)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	return logs, tx, nil
}

func (r *AuditPostgresRepository) MarkLogsProcessed(tx *sqlx.Tx, ids []int64) error {
	query := `
		UPDATE auditdb SET processed = TRUE
		WHERE id = ANY($1)
	`

	_, err := tx.Exec(query, ids)
	return err
}

func (r *AuditPostgresRepository) ResetFailedLogs(ctx context.Context, timeout time.Duration) error {
	query := `
		UPDATE auditdb SET processed = FALSE
		WHERE processed = TRUE AND created_at <= $1
	`

	timeThreshold := time.Now().Add(-timeout)
	_, err := r.db.ExecContext(ctx, query, timeThreshold)
	return err
}

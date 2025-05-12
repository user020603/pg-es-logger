package repositories

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"thanhnt208/pg-cdc-es/internal/models"
	"thanhnt208/pg-cdc-es/pkg/logger"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type IAuditESRepository interface {
	BulkIndexLogs(ctx context.Context, logs []models.ElasticAuditLog) error
}

type AuditESRepository struct {
	client *elasticsearch.Client
	index  string
	logger logger.ILogger
}

var _ IAuditESRepository = (*AuditESRepository)(nil)

func NewAuditESRepository(client *elasticsearch.Client, index string, logger logger.ILogger) (IAuditESRepository, error) {
	return &AuditESRepository{
		client: client,
		index:  index,
		logger: logger,
	}, nil
}

func (r *AuditESRepository) BulkIndexLogs(ctx context.Context, logs []models.ElasticAuditLog) error {
	if len(logs) == 0 {
		return nil
	}

	var buf bytes.Buffer

	for _, log := range logs {
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": fmt.Sprintf("%s-%s", r.index, time.Now().Format("2006.01.02")),
			},
		}

		if err := json.NewEncoder(&buf).Encode(meta); err != nil {
			r.logger.Error("failed to encode meta", "error", err)
			return err
		}

		if err := json.NewEncoder(&buf).Encode(log); err != nil {
			r.logger.Error("failed to encode log", "error", err)
			return err
		}
	}

	req := esapi.BulkRequest{
		Body:    bytes.NewReader(buf.Bytes()),
		Refresh: "false",
		Timeout: 30 * time.Second,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		r.logger.Error("failed to execute bulk request", "error", err)
		return err
	}
	defer res.Body.Close()

	var raw map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		r.logger.Error("failed to decode response", "error", err)
		return err
	}

	if res.IsError() {
		r.logger.Error("bulk index error", "status", res.StatusCode, "response", raw)
		return fmt.Errorf("elasticsearch status: %d, error: %v", res.StatusCode, raw)
	}

	if errorsField, ok := raw["errors"].(bool); ok && errorsField {
		r.logger.Error("bulk index partial failure", "response", raw)
		return fmt.Errorf("bulk index has errors: %v", raw)
	}

	r.logger.Info("bulk index logs successfully", "count", len(logs))
	return nil
}

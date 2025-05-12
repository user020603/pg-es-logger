package configs

import (
	"fmt"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
)

func ConnectElasticsearch() (*elasticsearch.Client, error) {
	addresses := os.Getenv("ES_ADDRESS")
	if addresses == "" {
		return nil, fmt.Errorf("missing required Elasticsearch environment variable: ES_ADDRESS")
	}
	addressList := strings.Split(addresses, ",")

	cfg := elasticsearch.Config{
		Addresses: addressList,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	_, err = client.Ping()
	if err != nil {
		return nil, fmt.Errorf("unable to ping Elasticsearch: %w", err)
	}

	return client, nil
}

package configs

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func ConnectKafkaWriter() (*kafka.Writer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")

	if brokers == "" {
		return nil, fmt.Errorf("missing required Kafka environment variable: KAFKA_BROKERS")
	}
	if topic == "" {
		return nil, fmt.Errorf("missing required Kafka environment variable: KAFKA_TOPIC")
	}

	brokerList := strings.Split(brokers, ",")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}

	return writer, nil
}

func ConnectKafkaReader() (*kafka.Reader, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")

	if brokers == "" {
		return nil, fmt.Errorf("missing required Kafka environment variable: KAFKA_BROKERS")
	}
	if topic == "" {
		return nil, fmt.Errorf("missing required Kafka environment variable: KAFKA_TOPIC")
	}
	if groupID == "" {
		return nil, fmt.Errorf("missing required Kafka environment variable: KAFKA_GROUP_ID")
	}

	brokerList := strings.Split(brokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerList,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return reader, nil
}

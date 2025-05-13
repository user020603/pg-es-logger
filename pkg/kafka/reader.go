package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafka.Reader
}

func NewKafkaReader(reader *kafka.Reader) IKafkaReader {
	return &KafkaReader{
		reader: reader,
	}
}

func (k *KafkaReader) Read(ctx context.Context) ([]byte, error) {
	msg, err := k.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	return msg.Value, nil
}

func (k *KafkaReader) Close() error {
	return k.reader.Close()
}

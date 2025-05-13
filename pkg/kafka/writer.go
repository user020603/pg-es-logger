package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	writer *kafka.Writer
}

func NewKafkaWriter(writer *kafka.Writer) IKafkaWriter {
	return &KafkaWriter{writer: writer}
}

func (k *KafkaWriter) Send(ctx context.Context, batch []byte) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Value: batch,
	})
}

func (k *KafkaWriter) Close() error {
	return k.writer.Close()
}

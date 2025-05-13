package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	writer *kafka.Writer
}

func NewKafkaWriter(brokers []string, topic string) IKafkaWriter {
	return &KafkaWriter{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (w *KafkaWriter) Send(ctx context.Context, batch []byte) error {
	msg := kafka.Message{
		Value: batch,
	}
	return w.writer.WriteMessages(ctx, msg)
}

func (w *KafkaWriter) Close() error {
	return w.writer.Close()
}

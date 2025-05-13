package kafka

import "context"

type IKafkaWriter interface {
	Send(ctx context.Context, batch []byte) error
	Close() error
}

type IKafkaReader interface {
	Read(ctx context.Context) ([]byte, error)
	Close() error
}
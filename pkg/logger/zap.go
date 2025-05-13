package logger

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ILogger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
	Fatal(msg string, keysAndValues ...interface{})
	Sync() error
}

var (
	instance ILogger
	once     sync.Once
)

type Logger struct {
	zap *zap.SugaredLogger
}

func NewLogger(level string) (ILogger, error) {
	once.Do(func() {
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

		var zapLevel zapcore.Level
		if err := zapLevel.Set(level); err != nil {
			zapLevel = zapcore.InfoLevel
		}

		core := zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.AddSync(os.Stdout),
			zapLevel,
		)

		logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
		instance = &Logger{
			zap: logger.Sugar(),
		}
	})

	if instance == nil {
		return nil, errors.New("logger instance is nil")
	}

	return instance, nil
}

func addLevelPrefix(level, msg string) string {
	return fmt.Sprintf("[%s] %s", strings.ToUpper(level), msg)
}

func (l *Logger) Debug(msg string, keysAndValues ...interface{}) {
	l.zap.Debugw(addLevelPrefix("DEBUG", msg), keysAndValues...)
}

func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	l.zap.Infow(addLevelPrefix("INFO", msg), keysAndValues...)
}

func (l *Logger) Warn(msg string, keysAndValues ...interface{}) {
	l.zap.Warnw(addLevelPrefix("WARN", msg), keysAndValues...)
}

func (l *Logger) Error(msg string, keysAndValues ...interface{}) {
	l.zap.Errorw(addLevelPrefix("ERROR", msg), keysAndValues...)
}

func (l *Logger) Fatal(msg string, keysAndValues ...interface{}) {
	l.zap.Fatalw(addLevelPrefix("FATAL", msg), keysAndValues...)
}

func (l *Logger) Sync() error {
	return l.zap.Sync()
}

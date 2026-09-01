// logger/logger.go
package logger

import (
    "encoding/json"
    "fmt"
    "os"
    "runtime"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

type LogLevel string

const (
    DebugLevel LogLevel = "debug"
    InfoLevel  LogLevel = "info"
    WarnLevel  LogLevel = "warn"
    ErrorLevel LogLevel = "error"
    FatalLevel LogLevel = "fatal"
)

type Logger struct {
    serviceName string
    zapLogger   *zap.Logger
    rabbitMQ    *amqp.Connection
    rabbitChan  *amqp.Channel
    queueName   string
}

type LogMessage struct {
    Timestamp   time.Time         `json:"timestamp"`
    Level       string           `json:"level"`
    Service     string           `json:"service"`
    Message     string           `json:"message"`
    Caller      string           `json:"caller"`
    StackTrace  string           `json:"stack_trace,omitempty"`
    TraceID     string           `json:"trace_id,omitempty"`
    RequestID   string           `json:"request_id,omitempty"`
    UserID      string           `json:"user_id,omitempty"`
    Additional  map[string]interface{} `json:"additional,omitempty"`
}

type Config struct {
    ServiceName      string
    LogLevel        LogLevel
    RabbitMQURL     string
    RabbitMQQueue   string
    LocalLogPath    string
}

// NewLogger creates a new logger instance
func NewLogger(config Config) (*Logger, error) {
    // Create encoder configuration
    encoderConfig := zapcore.EncoderConfig{
        TimeKey:        "timestamp",
        LevelKey:       "level",
        NameKey:        "logger",
        CallerKey:      "caller",
        MessageKey:     "message",
        StacktraceKey:  "stacktrace",
        LineEnding:     zapcore.DefaultLineEnding,
        EncodeLevel:    zapcore.LowercaseLevelEncoder,
        EncodeTime:     zapcore.ISO8601TimeEncoder,
        EncodeDuration: zapcore.SecondsDurationEncoder,
        EncodeCaller:   zapcore.ShortCallerEncoder,
    }

    // Create core that writes to both file and console
    fileWriter, err := os.OpenFile(config.LocalLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil, fmt.Errorf("failed to open log file: %v", err)
    }

    // Create cores for both console and file output
    consoleCore := zapcore.NewCore(
        zapcore.NewJSONEncoder(encoderConfig),
        zapcore.AddSync(os.Stdout),
        getZapLevel(config.LogLevel),
    )

    fileCore := zapcore.NewCore(
        zapcore.NewJSONEncoder(encoderConfig),
        zapcore.AddSync(fileWriter),
        getZapLevel(config.LogLevel),
    )

    // Combine cores
    core := zapcore.NewTee(consoleCore, fileCore)

    // Create logger
    zapLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

    // Connect to RabbitMQ
    conn, err := amqp.Dial(config.RabbitMQURL)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to open channel: %v", err)
    }

    // Declare queue
    _, err = ch.QueueDeclare(
        config.RabbitMQQueue,
        true,  // durable
        false, // auto-delete
        false, // exclusive
        false, // no-wait
        nil,   // arguments
    )
    if err != nil {
        ch.Close()
        conn.Close()
        return nil, fmt.Errorf("failed to declare queue: %v", err)
    }

    return &Logger{
        serviceName: config.ServiceName,
        zapLogger:   zapLogger,
        rabbitMQ:    conn,
        rabbitChan:  ch,
        queueName:   config.RabbitMQQueue,
    }, nil
}

func (l *Logger) log(level LogLevel, msg string, fields map[string]interface{}) {
    // Create log message
    logMsg := LogMessage{
        Timestamp:   time.Now(),
        Level:       string(level),
        Service:     l.serviceName,
        Message:     msg,
        Additional:  fields,
    }

    // Add caller information
    if _, file, line, ok := runtime.Caller(2); ok {
        logMsg.Caller = fmt.Sprintf("%s:%d", file, line)
    }

    // Convert to JSON
    jsonMsg, err := json.Marshal(logMsg)
    if err != nil {
        l.zapLogger.Error("failed to marshal log message", zap.Error(err))
        return
    }

    // Publish to RabbitMQ
    err = l.rabbitChan.Publish(
        "",           // exchange
        l.queueName,  // routing key
        false,        // mandatory
        false,        // immediate
        amqp.Publishing{
            ContentType: "application/json",
            Body:        jsonMsg,
        },
    )
    if err != nil {
        l.zapLogger.Error("failed to publish to RabbitMQ", zap.Error(err))
    }

    // Log using zap logger
    zapFields := make([]zap.Field, 0)
    for k, v := range fields {
        zapFields = append(zapFields, zap.Any(k, v))
    }

    switch level {
    case DebugLevel:
        l.zapLogger.Debug(msg, zapFields...)
    case InfoLevel:
        l.zapLogger.Info(msg, zapFields...)
    case WarnLevel:
        l.zapLogger.Warn(msg, zapFields...)
    case ErrorLevel:
        l.zapLogger.Error(msg, zapFields...)
    case FatalLevel:
        l.zapLogger.Fatal(msg, zapFields...)
    }
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, fields map[string]interface{}) {
    l.log(DebugLevel, msg, fields)
}

// Info logs an info message
func (l *Logger) Info(msg string, fields map[string]interface{}) {
    l.log(InfoLevel, msg, fields)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, fields map[string]interface{}) {
    l.log(WarnLevel, msg, fields)
}

// Error logs an error message
func (l *Logger) Error(msg string, fields map[string]interface{}) {
    l.log(ErrorLevel, msg, fields)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string, fields map[string]interface{}) {
    l.log(FatalLevel, msg, fields)
    os.Exit(1)
}

// Close closes the logger and its connections
func (l *Logger) Close() {
    if l.zapLogger != nil {
        l.zapLogger.Sync()
    }
    if l.rabbitChan != nil {
        l.rabbitChan.Close()
    }
    if l.rabbitMQ != nil {
        l.rabbitMQ.Close()
    }
}

func getZapLevel(level LogLevel) zapcore.Level {
    switch level {
    case DebugLevel:
        return zapcore.DebugLevel
    case InfoLevel:
        return zapcore.InfoLevel
    case WarnLevel:
        return zapcore.WarnLevel
    case ErrorLevel:
        return zapcore.ErrorLevel
    case FatalLevel:
        return zapcore.FatalLevel
    default:
        return zapcore.InfoLevel
    }
}
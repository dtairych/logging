// internal/storage/postgres.go
package storage

import (
    "context"
    "fmt"
    "time"

    "github.com/jackc/pgx/v4/pgxpool"
    "github.com/yourusername/log-consumer/internal/models"
)

// StorageHandler handles log persistence operations
type StorageHandler struct {
    pool *pgxpool.Pool
}

// Config holds database configuration
type Config struct {
    Host         string
    Port         int
    User         string
    Password     string
    Database     string
    MaxConns     int32
    MinConns     int32
    MaxConnLife  time.Duration
    MaxConnIdle  time.Duration
}

// NewStorageHandler creates a new storage handler
func NewStorageHandler(ctx context.Context, cfg Config) (*StorageHandler, error) {
    connStr := fmt.Sprintf(
        "host=%s port=%d user=%s password=%s dbname=%s pool_max_conns=%d pool_min_conns=%d",
        cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.MaxConns, cfg.MinConns,
    )

    poolConfig, err := pgxpool.ParseConfig(connStr)
    if err != nil {
        return nil, fmt.Errorf("error parsing config: %w", err)
    }

    // Configure connection pool
    poolConfig.MaxConnLifetime = cfg.MaxConnLife
    poolConfig.MaxConnIdleTime = cfg.MaxConnIdle

    // Create connection pool
    pool, err := pgxpool.ConnectConfig(ctx, poolConfig)
    if err != nil {
        return nil, fmt.Errorf("error connecting to database: %w", err)
    }

    // Initialize schema
    if err := initializeSchema(ctx, pool); err != nil {
        pool.Close()
        return nil, fmt.Errorf("error initializing schema: %w", err)
    }

    return &StorageHandler{pool: pool}, nil
}

// initializeSchema ensures the required tables exist
func initializeSchema(ctx context.Context, pool *pgxpool.Pool) error {
    query := `
    CREATE TABLE IF NOT EXISTS logs (
        id BIGSERIAL PRIMARY KEY,
        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        level VARCHAR(10) NOT NULL,
        message TEXT NOT NULL,
        service_name VARCHAR(100) NOT NULL,
        trace_id VARCHAR(100),
        file VARCHAR(255),
        line INTEGER,
        data JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        -- Add indexes for common query patterns
        CONSTRAINT logs_level_check CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR'))
    );

    CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
    CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
    CREATE INDEX IF NOT EXISTS idx_logs_service_name ON logs(service_name);
    CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id);
    CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);
    `

    _, err := pool.Exec(ctx, query)
    return err
}

// StoreBatch stores a batch of log messages
func (s *StorageHandler) StoreBatch(ctx context.Context, batch *models.LogBatch) error {
    // Begin transaction
    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return fmt.Errorf("error beginning transaction: %w", err)
    }
    defer tx.Rollback(ctx) // rollback if not committed

    // Prepare batch insert
    const query = `
        INSERT INTO logs (
            timestamp, level, message, service_name, 
            trace_id, file, line, data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `

    // Execute batch insert
    for _, msg := range batch.Messages {
        _, err := tx.Exec(ctx, query,
            msg.Timestamp,
            msg.Level,
            msg.Message,
            msg.ServiceName,
            msg.TraceID,
            msg.File,
            msg.Line,
            msg.Data,
        )
        if err != nil {
            return fmt.Errorf("error inserting log message: %w", err)
        }
    }

    // Commit transaction
    if err := tx.Commit(ctx); err != nil {
        return fmt.Errorf("error committing transaction: %w", err)
    }

    return nil
}

// QueryLogs retrieves logs based on filters
func (s *StorageHandler) QueryLogs(ctx context.Context, params QueryParams) ([]models.LogMessage, error) {
    query := `
        SELECT timestamp, level, message, service_name, trace_id, file, line, data
        FROM logs
        WHERE ($1::timestamp IS NULL OR timestamp >= $1)
        AND ($2::timestamp IS NULL OR timestamp <= $2)
        AND ($3::varchar IS NULL OR level = $3)
        AND ($4::varchar IS NULL OR service_name = $4)
        AND ($5::varchar IS NULL OR trace_id = $5)
        ORDER BY timestamp DESC
        LIMIT $6 OFFSET $7
    `

    rows, err := s.pool.Query(ctx, query,
        params.StartTime,
        params.EndTime,
        params.Level,
        params.ServiceName,
        params.TraceID,
        params.Limit,
        params.Offset,
    )
    if err != nil {
        return nil, fmt.Errorf("error querying logs: %w", err)
    }
    defer rows.Close()

    var logs []models.LogMessage
    for rows.Next() {
        var log models.LogMessage
        err := rows.Scan(
            &log.Timestamp,
            &log.Level,
            &log.Message,
            &log.ServiceName,
            &log.TraceID,
            &log.File,
            &log.Line,
            &log.Data,
        )
        if err != nil {
            return nil, fmt.Errorf("error scanning log row: %w", err)
        }
        logs = append(logs, log)
    }

    return logs, nil
}

// QueryParams defines parameters for querying logs
type QueryParams struct {
    StartTime   *time.Time
    EndTime     *time.Time
    Level       *string
    ServiceName *string
    TraceID     *string
    Limit       int
    Offset      int
}

// Close closes the database connection pool
func (s *StorageHandler) Close() {
    if s.pool != nil {
        s.pool.Close()
    }
}
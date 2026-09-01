// internal/consumer/rabbitmq.go
package consumer

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "sync"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/yourusername/log-consumer/internal/models"
    "github.com/yourusername/log-consumer/internal/storage"
)

// [Previous code remains the same until processBatches...]

func (c *Consumer) processBatches(ctx context.Context, messages <-chan amqp.Delivery) {
    defer c.wg.Done()

    batch := models.NewLogBatch(c.batchSize)
    ticker := time.NewTicker(c.batchTimeout)
    defer ticker.Stop()

    // Keep track of messages in the current batch for acknowledgment
    var msgBatch []amqp.Delivery

    // flushBatch handles storing the current batch and acknowledging messages
    flushBatch := func() {
        if batch.Size > 0 {
            if err := c.storage.StoreBatch(ctx, batch); err != nil {
                log.Printf("Error storing batch: %v", err)
                // Nack all messages in batch for requeuing
                for _, msg := range msgBatch {
                    msg.Nack(false, true) // requeue messages on storage failure
                }
            } else {
                // Successfully stored - ack all messages
                for _, msg := range msgBatch {
                    msg.Ack(false) // false = don't ack multiple messages
                }
            }
            // Clear the batches
            batch.Clear()
            msgBatch = msgBatch[:0]
        }
    }

    for {
        select {
        case <-ctx.Done():
            // Context cancelled, flush remaining messages
            flushBatch()
            return

        case <-c.quit:
            // Graceful shutdown requested
            flushBatch()
            return

        case <-ticker.C:
            // Timeout reached, flush current batch even if not full
            flushBatch()

        case msg, ok := <-messages:
            if !ok {
                // Channel closed
                flushBatch()
                return
            }

            var logMsg models.LogMessage
            if err := json.Unmarshal(msg.Body, &logMsg); err != nil {
                log.Printf("Error unmarshaling message: %v", err)
                msg.Nack(false, false) // Don't requeue malformed messages
                continue
            }

            // Add to batch
            if !batch.Add(logMsg) {
                // Batch is full, store it
                flushBatch()
                // Add the message to the new batch
                batch.Add(logMsg)
            }
            
            // Track message for acknowledgment
            msgBatch = append(msgBatch, msg)
        }
    }
}

// reconnect handles reconnection to RabbitMQ with exponential backoff
func (c *Consumer) reconnect() error {
    backoff := time.Second
    maxBackoff := time.Minute
    for {
        // Try to connect
        conn, err := amqp.Dial(c.config.URL)
        if err == nil {
            channel, err := conn.Channel()
            if err == nil {
                // Update connection and channel
                if c.conn != nil {
                    c.conn.Close()
                }
                if c.channel != nil {
                    c.channel.Close()
                }
                c.conn = conn
                c.channel = channel
                
                // Redeclare exchange and queue
                err = c.setupTopology()
                if err == nil {
                    return nil // Successfully reconnected
                }
            }
            conn.Close()
        }

        log.Printf("Failed to reconnect: %v, retrying in %v", err, backoff)
        time.Sleep(backoff)
        
        // Exponential backoff with maximum
        backoff *= 2
        if backoff > maxBackoff {
            backoff = maxBackoff
        }
    }
}

// setupTopology declares the exchange, queue, and bindings
func (c *Consumer) setupTopology() error {
    // Declare exchange
    err := c.channel.ExchangeDeclare(
        c.config.Exchange,
        c.config.ExchangeType,
        true,  // durable
        false, // auto-deleted
        false, // internal
        false, // no-wait
        nil,   // arguments
    )
    if err != nil {
        return fmt.Errorf("error declaring exchange: %w", err)
    }

    // Declare queue
    queue, err := c.channel.QueueDeclare(
        c.config.Queue,
        true,  // durable
        false, // delete when unused
        false, // exclusive
        false, // no-wait
        nil,   // arguments
    )
    if err != nil {
        return fmt.Errorf("error declaring queue: %w", err)
    }

    // Bind queue
    err = c.channel.QueueBind(
        queue.Name,
        c.config.RoutingKey,
        c.config.Exchange,
        false, // no-wait
        nil,   // arguments
    )
    if err != nil {
        return fmt.Errorf("error binding queue: %w", err)
    }

    return nil
}

// Shutdown gracefully shuts down the consumer
func (c *Consumer) Shutdown(ctx context.Context) error {
    // Signal processBatches to stop
    close(c.quit)

    // Wait for processing to complete with timeout
    done := make(chan struct{})
    go func() {
        c.wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        // Graceful shutdown completed
    case <-ctx.Done():
        return fmt.Errorf("shutdown timed out")
    }

    // Close connections
    if err := c.channel.Close(); err != nil {
        return fmt.Errorf("error closing channel: %w", err)
    }
    if err := c.conn.Close(); err != nil {
        return fmt.Errorf("error closing connection: %w", err)
    }

    return nil
}

// Stats provides consumer statistics
type Stats struct {
    MessagesProcessed uint64
    BatchesProcessed  uint64
    Errors           uint64
    LastError        string
    LastErrorTime    time.Time
}

// GetStats returns current consumer statistics
func (c *Consumer) GetStats() Stats {
    // Implementation would track these metrics during processing
    return Stats{
        // Add actual stats tracking
    }
}

func main() {
    // Initialize storage
    storageHandler, err := storage.NewStorageHandler(ctx, storageConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer storageHandler.Close()

    // Create consumer
    consumer, err := consumer.NewConsumer(
        consumer.Config{
            URL:          "amqp://localhost:5672",
            Exchange:     "logs",
            ExchangeType: "topic",
            Queue:        "log_queue",
            RoutingKey:   "logs.*",
            BatchSize:    100,
            BatchTimeout: time.Second * 5,
        },
        storageHandler,
    )
    if err != nil {
        log.Fatal(err)
    }

    // Start consuming
    if err := consumer.Start(ctx); err != nil {
        log.Fatal(err)
    }

    // Wait for shutdown signal
    <-signalChan
    
    // Graceful shutdown
    shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
    defer cancel()
    if err := consumer.Shutdown(shutdownCtx); err != nil {
        log.Printf("Error during shutdown: %v", err)
    }
}
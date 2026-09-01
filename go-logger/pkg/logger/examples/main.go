// go-logger/pkg/logger/examples/basic/main.go
package main

import (
    "context"
    "github.com/yourusername/go-logger/pkg/logger"
)

func main() {
    log, err := logger.New(logger.Config{
        ServiceName:  "example-service",
        RabbitMQURL: "amqp://localhost:5672",
        Exchange:    "logs",
        RoutingKey:  "service.logs",
    })
    if err != nil {
        panic(err)
    }
    defer log.Close()

    ctx := context.Background()
    log.Info(ctx, "Hello from the logger!")
}
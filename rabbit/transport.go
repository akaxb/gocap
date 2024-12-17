package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/akaxb/gocap/core"
	"github.com/akaxb/gocap/model"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
	"time"
)

type Transport struct {
	pool   *ConnectionChannelPool
	logger *log.Logger
}

var _ core.ITransport = (*Transport)(nil)

func NewTransport(logger *log.Logger, rabbitMQOptions RabbitmqOptions, opts ...option) *Transport {
	t := &Transport{
		logger: logger,
		pool:   NewConnectionChannelPool(logger, rabbitMQOptions),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

type option func(r *Transport)

func (r *Transport) Send(name string, message *model.MediumMessage) error {
	ch, err := r.pool.Rent()
	if err != nil {
		return fmt.Errorf("failed to rent a channel from pool: %w", err)
	}
	defer func() {
		err := r.pool.Return(ch)
		if err != nil {
			r.logger.Println("Failed to return channel to pool:", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, err := json.Marshal(message.Origin.Value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	msg := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         body,
		MessageId:    strconv.FormatInt(message.Id, 10),
		DeliveryMode: 2,
	}
	err = ch.PublishWithContext(ctx, r.pool.exchange, name, false, false, msg)
	if err != nil {
		r.logger.Println("Failed to publish a message:", err)
		return err
	}
	r.logger.Printf(" [x] Sent %s\n", string(body))
	return nil
}

func (r *Transport) Close() error {
	r.logger.Println("Closing transport")
	r.pool.Dispose()
	r.logger.Println("Transport closed")
	return nil
}

package amqphelper

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

type rpcResponse struct {
	data chan []byte
	err  chan error
}

func (r *rpcResponse) Close() {
	close(r.data)
	close(r.err)
}

type RPCHandler struct {
	channel *Channel

	infligthMtx sync.Mutex
	inflight    map[string]*rpcResponse

	queueName string

	closed bool
}

func CreateRPCHandler(connection *amqp091.Connection) (*RPCHandler, error) {
	queueName := uuid.NewString()

	channel, err := CreateNewChannelPair(connection)
	if err != nil {
		return nil, err
	}

	_, err = channel.consumeChannel.QueueDeclare(queueName, false, true, true, true, amqp091.Table{})
	if err != nil {
		channel.Close()
		return nil, err
	}

	handler := &RPCHandler{
		channel: channel,

		infligthMtx: sync.Mutex{},
		inflight:    make(map[string]*rpcResponse),

		queueName: queueName,
		closed:    false,
	}

	go handler.consume()

	return handler, nil
}

func (c *RPCHandler) consume() {
	deliveries, err := c.channel.consumeChannel.Consume(c.queueName, uuid.NewString(), false, true, true, true, amqp091.Table{})
	if err != nil {
		c.Close()
		return
	}
	for delivery := range deliveries {
		c.infligthMtx.Lock()
		if val, ok := c.inflight[delivery.CorrelationId]; ok {
			val.data <- delivery.Body
			delivery.Ack(false)
			delete(c.inflight, delivery.CorrelationId)
		} else {
			delivery.Nack(false, true)
		}
		c.infligthMtx.Unlock()
	}
	c.Close()
}

func (c *RPCHandler) Invoke(ctx context.Context, exchange string, key string, data []byte, headers amqp091.Table) ([]byte, error) {
	if c.closed {
		return nil, ErrChannelClosed
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	correlationId := uuid.NewString()

	msg := amqp091.Publishing{
		Headers:       headers,
		CorrelationId: correlationId,
		Timestamp:     time.Now(),
		ReplyTo:       c.queueName,
		Body:          data,
	}
	err := c.channel.publishChannel.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		return nil, err
	}

	response := &rpcResponse{
		data: make(chan []byte),
		err:  make(chan error),
	}
	defer response.Close()
	c.infligthMtx.Lock()
	c.inflight[correlationId] = response
	c.infligthMtx.Unlock()

	select {
	case result := <-response.data:
		return result, nil
	case err := <-response.err:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *RPCHandler) NotifyClose(ch chan<- error) {
	c.channel.NotifyClose(ch)
}

func (c *RPCHandler) Close() error {
	c.infligthMtx.Lock()
	defer c.infligthMtx.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	err := c.channel.Close()

	// drain inflight requests
	for _, val := range c.inflight {
		val.err <- errors.Join(ErrChannelClosed, err)
	}
	return err
}

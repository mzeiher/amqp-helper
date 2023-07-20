package amqphelper

import (
	"context"
	"runtime"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

type Handler struct {
	channel *Channel

	exchange   string
	queue      string
	key        string
	autoDelete bool
	exclusive  bool

	handlerFunc HandlerFunc
}

type HandlerFunc func(data []byte, header amqp091.Table) ([]byte, error)

func CreateHandler(connection *amqp091.Connection, exchange string, queue string, key string, autoDelete bool, exclusive bool, handlerFunc HandlerFunc) (*Handler, error) {

	channel, err := CreateNewChannelPair(connection)
	if err != nil {
		return nil, err
	}

	_, err = channel.consumeChannel.QueueDeclare(queue, true, autoDelete, exclusive, true, amqp091.Table{})
	if err != nil {
		return nil, err
	}
	if exchange != "" {
		err = channel.consumeChannel.QueueBind(queue, key, exchange, true, amqp091.Table{})
		if err != nil {
			return nil, err
		}
	}

	handler := &Handler{
		channel:     channel,
		handlerFunc: handlerFunc,
		exchange:    exchange,
		queue:       queue,
		key:         key,
		autoDelete:  autoDelete,
		exclusive:   exclusive,
	}
	go handler.consume()
	return handler, nil
}

func (h *Handler) consume() {
	delieries, err := h.channel.consumeChannel.Consume(h.queue, uuid.NewString(), false, h.exclusive, true, true, amqp091.Table{})
	if err != nil {
		h.Close()
		return
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for delivery := range delieries {
				result, err := h.handlerFunc(delivery.Body, delivery.Headers)
				if delivery.ReplyTo != "" {
					msg := amqp091.Publishing{
						Headers:       amqp091.Table{},
						CorrelationId: delivery.CorrelationId,
						Body:          result,
					}
					if err != nil {
						msg.Headers["error"] = err.Error()
					}
					h.channel.publishChannel.PublishWithContext(context.Background(), "", delivery.ReplyTo, false, false, msg)
				}
				delivery.Ack(false)

			}
		}()
	}
}

func (h *Handler) NotifyClose(ch chan<- error) {
	h.channel.NotifyClose(ch)
}

func (h *Handler) Close() error {
	return h.channel.Close()
}

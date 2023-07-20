package amqphelper

import (
	"errors"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

var ErrChannelClosed = errors.New("channel closed")

type Channel struct {
	consumeChannel *amqp091.Channel
	publishChannel *amqp091.Channel
	notifyMutext   sync.Mutex
	notifyClose    []chan<- error

	reason error
	closed bool
}

func CreateNewChannelPair(connection *amqp091.Connection) (*Channel, error) {

	consumeChannel, err := connection.Channel()
	if err != nil {
		return nil, err
	}
	publishChannel, err := connection.Channel()
	if err != nil {
		consumeChannel.Close()
		return nil, err
	}

	notifyConsumeClose := make(chan *amqp091.Error)
	consumeChannel.NotifyClose(notifyConsumeClose)
	notifyPublishClose := make(chan *amqp091.Error)
	publishChannel.NotifyClose(notifyPublishClose)

	channel := &Channel{
		consumeChannel: consumeChannel,
		publishChannel: publishChannel,

		notifyMutext: sync.Mutex{},
		notifyClose:  make([]chan<- error, 0),

		closed: false,
		reason: nil,
	}

	notifyError := make(chan error)
	signalClose := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for consumeClosedError := range notifyConsumeClose {
			notifyError <- consumeClosedError
		}
		signalClose <- struct{}{}
		wg.Done()
	}()
	go func() {
		for publishClosedError := range notifyPublishClose {
			notifyError <- publishClosedError
		}
		signalClose <- struct{}{}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(notifyError)
		close(signalClose)
	}()

	go func() {
		for range signalClose {
			channel.Close()
		}
	}()

	go func() {
		for err := range notifyError {
			channel.reason = errors.Join(channel.reason, err)
		}
	}()

	return channel, nil
}

func (c *Channel) ConsumeChannel() (*amqp091.Channel, error) {
	if c.closed {
		return nil, errors.Join(ErrChannelClosed, c.reason)
	}
	return c.consumeChannel, nil
}

func (c *Channel) PublishChannel() (*amqp091.Channel, error) {
	if c.closed {
		return nil, errors.Join(ErrChannelClosed, c.reason)
	}
	return c.publishChannel, nil
}

func (c *Channel) NotifyClose(ch chan<- error) {
	c.notifyMutext.Lock()
	defer c.notifyMutext.Unlock()
	if !c.closed {
		c.notifyClose = append(c.notifyClose, ch)
	}
}

func (c *Channel) IsClose() bool {
	return c.closed
}

func (c *Channel) Close() error {
	c.notifyMutext.Lock()
	defer c.notifyMutext.Unlock()

	if c.closed {
		return c.reason
	}
	c.closed = true
	c.reason = errors.Join(c.reason, c.consumeChannel.Close(), c.publishChannel.Close())

	for _, ch := range c.notifyClose {
		ch <- c.reason
		close(ch)
	}

	return c.reason
}

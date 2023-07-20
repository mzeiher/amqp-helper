package amqphelper_test

import (
	"fmt"
	"testing"

	amqphelper "github.com/mzeiher/amqp-helper"
	"github.com/rabbitmq/amqp091-go"
)

func TestChannelCloseByConsumeChannel(t *testing.T) {
	connection, err := dialRabbitMq()
	if err != nil {
		t.Fatal(err)
	}
	channel, err := amqphelper.CreateNewChannelPair(connection)
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()

	consumeChannel, err := channel.ConsumeChannel()
	if err != nil {
		t.Fatal(err)
	}

	notifyClose := make(chan error)
	channel.NotifyClose(notifyClose)

	consumeChannel.Close()

	err = <-notifyClose
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestChannelCloseByPublishChannel(t *testing.T) {
	connection, err := dialRabbitMq()
	if err != nil {
		t.Fatal(err)
	}
	channel, err := amqphelper.CreateNewChannelPair(connection)
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()

	publishChannel, err := channel.PublishChannel()
	if err != nil {
		t.Fatal(err)
	}

	notifyClose := make(chan error)
	channel.NotifyClose(notifyClose)

	publishChannel.Close()

	err = <-notifyClose
	if err == nil {
		t.Fatalf("expected error")
	}

}

func TestChannelGracefullClose(t *testing.T) {
	connection, err := dialRabbitMq()
	if err != nil {
		t.Fatal(err)
	}
	channel, err := amqphelper.CreateNewChannelPair(connection)
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()

	notifyClose := make(chan error, 1)
	channel.NotifyClose(notifyClose)

	channel.Close()

	err = <-notifyClose
	if err != nil {
		t.Fatal(err)
	}

}

func dialRabbitMq() (*amqp091.Connection, error) {
	return amqp091.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", "guest", "guest", "localhost", 5672))
}

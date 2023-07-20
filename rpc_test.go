package amqphelper_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	amqphelper "github.com/mzeiher/amqp-helper"
	"github.com/rabbitmq/amqp091-go"
)

func TestRPC(t *testing.T) {
	connection, err := dialRabbitMq()
	defer func() {
		connection.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}

	queue := uuid.NewString()
	handler, err := amqphelper.CreateHandler(connection, "", queue, "", true, true, func(data []byte, header amqp091.Table) ([]byte, error) {
		return data, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()

	rpc, err := amqphelper.CreateRPCHandler(connection)
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()

	result, err := rpc.Invoke(context.Background(), "", queue, []byte("hello world"), amqp091.Table{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result, []byte("hello world")) {
		t.Fatalf("invalid result")
	}
}

func BenchmarkRPC100(b *testing.B)   { benchmarkRPC(100, b) }
func BenchmarkRPC1000(b *testing.B)  { benchmarkRPC(1000, b) }
func BenchmarkRPC10000(b *testing.B) { benchmarkRPC(10000, b) }

func benchmarkRPC(messages int, b *testing.B) {
	connection, err := dialRabbitMq()
	if err != nil {
		b.Fatal(err)
	}
	defer connection.Close()

	queue := uuid.NewString()
	handler, err := amqphelper.CreateHandler(connection, "", queue, "", true, true, func(data []byte, header amqp091.Table) ([]byte, error) {
		return data, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer handler.Close()

	rpc, err := amqphelper.CreateRPCHandler(connection)
	if err != nil {
		b.Fatal(err)
	}
	defer rpc.Close()

	for i := 0; i < b.N; i++ {
		for m := 0; m < messages; m++ {
			_, err = rpc.Invoke(context.Background(), "", queue, []byte("hello world"), amqp091.Table{})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

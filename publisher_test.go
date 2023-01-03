package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
	"os/exec"
	"testing"
	"time"
)

func TestPublisher_Publish(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Error(err)
	}

	// server
	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	messageReceived := false
	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		ctx.Ack()
		messageReceived = true
	})

	server := NewServer(conn, router)
	err = startServerAsync(server)
	if err != nil {
		t.Error(err)
	}

	// publisher
	publisher, err := NewPublisher(conn, exchangeParams, WithQueueDeclaration(queueParams, "test.foo"), WithRetries(NewDefaultSigmoidBackoff(), 10))
	if err != nil {
		t.Error(err)
	}

	err = publisher.Publish(context.Background(), "test.foo", false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         []byte("body"),
		DeliveryMode: 2,
	})
	if err != nil {
		t.Error(err)
	}

	// wait for publish
	time.Sleep(1 * time.Second)

	if !messageReceived {
		t.Error("message was not received on server side")
	}
}

func TestPublisher_Broken(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Error(err)
	}

	publisher, err := NewPublisher(conn, exchangeParams, WithQueueDeclaration(queueParams, "test.foo"), WithRetries(NewDefaultSigmoidBackoff(), 10))
	if err != nil {
		t.Error(err)
	}

	cmd := exec.Command(stopRabbitServerCommand, stopRabbitServerCommandArguments...)
	err = cmd.Run()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		if publisher.Broken() {
			t.Error("Publisher is broken")
		}

		go func() {
			err = publisher.Publish(context.Background(), "test.foo", false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("body"),
			})
			if err != nil {
				t.Error(err)
			}
		}()

		// wait for publish in goroutine
		time.Sleep(100 * time.Millisecond)
	}

	// wait for errors to accumulate
	time.Sleep(5 * time.Second)

	if !publisher.Broken() {
		t.Error("Publisher is not broken after expected errors")
	}

	err = restartRabbitAndWaitReconnect(7)
	if err != nil {
		t.Error(err)
	}

	if publisher.Broken() {
		t.Error("Publisher is broken")
	}
}

func TestPublisher_Reconnect(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Error(err)
	}

	// server
	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	messageReceived := false
	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		ctx.Ack()
		messageReceived = true
	})

	server := NewServer(conn, router)
	err = startServerAsync(server)
	if err != nil {
		t.Error(err)
	}

	// publisher
	publisher, err := NewPublisher(conn, exchangeParams, WithQueueDeclaration(queueParams, "test.foo"), WithRetries(NewDefaultSigmoidBackoff(), 10))
	if err != nil {
		t.Error(err)
	}

	err = restartRabbitAndWaitReconnect(7)
	if err != nil {
		t.Error(err)
	}

	err = publisher.Publish(context.Background(), "test.foo", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("body"),
	})
	if err != nil {
		t.Error(err)
	}

	// wait for message publish
	time.Sleep(1 * time.Second)

	if !messageReceived {
		t.Error("message was not received on server side")
	}
}

package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

var exchangeParams = ExchangeParams{
	Name:       "test",
	Kind:       "direct",
	AutoDelete: true,
}

var queueParams = QueueParams{
	Name:       "test",
	AutoDelete: true,
}

var qos = QualityOfService{
	PrefetchCount: 5,
}

var consumer = ConsumerParams{
	ConsumerName: "test",
	AutoAck:      false,
	ConsumerArgs: nil,
}

func startServerAsync(server *Server) error {
	errChan := make(chan error)
	go func() {
		err := server.ListenAndServe(context.Background())
		if err != nil {
			errChan <- err
		}
	}()

	// wait for server startup
	time.Sleep(1 * time.Second)

	// check for ListenAndServe error
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func TestServer_ListenAndServe(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Error(err)
	}

	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	fooMessage := []byte("foo")
	fooMessageReceived := false
	fooController := func(ctx *DeliveryContext) {
		if string(ctx.Delivery.Body) != string(fooMessage) {
			t.Errorf("unexpected message received in foo %s", ctx.Delivery.Body)
		}

		err = ctx.Delivery.Ack(false)
		if err != nil {
			t.Error(err)
		}

		fooMessageReceived = true
	}

	barMessage := []byte("bar")
	barMessageReceived := false
	barController := func(ctx *DeliveryContext) {
		if string(ctx.Delivery.Body) != string(barMessage) {
			t.Errorf("unexpected message received in bar %s", ctx.Delivery.Body)
		}

		err = ctx.Delivery.Ack(false)
		if err != nil {
			t.Error(err)
		}

		barMessageReceived = true
	}

	routerGroup.Route("test.foo", fooController)
	routerGroup.Route("test.bar", barController)

	server := NewServer(conn, router)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())

	err = startServerAsync(server)
	if err != nil {
		t.Error(err)
	}

	publisherCh, err := conn.Channel()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		err = publisherCh.Publish(exchangeParams.Name, "test.foo", false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        fooMessage,
		})
		if err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < 10; i++ {
		err = publisherCh.Publish(exchangeParams.Name, "test.bar", false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         barMessage,
			DeliveryMode: 2,
		})
		if err != nil {
			t.Error(err)
		}
	}

	// wait for messages to arrive
	time.Sleep(1 * time.Second)

	if !fooMessageReceived {
		t.Error("foo message was not received")
	}
	if !barMessageReceived {
		t.Error("bar message was not received")
	}
}

func TestServer_Shutdown(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Error(err)
	}

	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	gracefulStopCompleted := false
	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		// simulate message processing time
		time.Sleep(5 * time.Second)
		gracefulStopCompleted = true
	})

	server := NewServer(conn, router)
	if err != nil {
		t.Fatal(err)
	}

	err = startServerAsync(server)
	if err != nil {
		t.Error(err)
	}

	publisherCh, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	err = publisherCh.Publish(exchangeParams.Name, "test.foo", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("message"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// wait for publish
	time.Sleep(1 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !gracefulStopCompleted {
		t.Error("graceful stop did not complete")
	}
}

func TestServer_Reconnect(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	router := NewRouter()
	routerGroup := router.Group(
		ExchangeParams{
			Name:       "test-durable",
			Kind:       "topic",
			Durable:    true,
			AutoDelete: false,
		}, QueueParams{
			Name:       "test-durable",
			Durable:    true,
			AutoDelete: false,
		}, qos, consumer,
	)

	messageReceivedFirstTime := false
	messageReceivedSecondTime := false

	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		// simulate message processing time
		time.Sleep(1 * time.Second)

		if !messageReceivedFirstTime {
			messageReceivedFirstTime = true
			// test send after server is restarted on message processing
			go func() {
				err = restartRabbitAndWaitReconnect(7)
				if err != nil {
					t.Error(err)
				}
			}()

			// wait for server stop
			time.Sleep(1 * time.Second)
		}

		if !ctx.Ack() {
			return
		}

		messageReceivedSecondTime = true
	})

	server := NewServer(conn, router)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())

	err = startServerAsync(server)
	if err != nil {
		t.Error(err)
	}

	publisher, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	err = publisher.Publish("test-durable", "test.foo", false, false, amqp.Publishing{
		DeliveryMode: 2,
		ContentType:  "text/plain",
		Body:         []byte("message"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// wait for message to be processed with server shutdown while processing, restarted and processed again
	<-time.After(15 * time.Second)

	err = server.Shutdown(context.Background())
	if err != nil {
		t.Error(err)
	}

	if !messageReceivedFirstTime {
		t.Error("message was not received first time")
	}
	if !messageReceivedSecondTime {
		t.Error("message was not received second time")
	}
}

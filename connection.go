package rabbitmq

import (
	"context"
	"github.com/MashinIvan/rabbitmq/pkg/backoff"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"sync/atomic"
)

// ConnectionFactory is a wrapper used for reconnecting to rabbitmq server.
type ConnectionFactory func() (*amqp.Connection, error)

// NewConnection creates a new *Connection as in NewConnectionWithContext, but uses context.Background.
func NewConnection(factory ConnectionFactory, backoff backoff.Backoff) (*Connection, error) {
	return NewConnectionWithContext(context.Background(), factory, backoff)
}

// NewConnectionWithContext accepts connection factory and a backoff returning *Connection.
// When amqp connection is closed unexpectedly, Connection attempts to reconnect with backoff.
// Context is passed to backoff to cancel reconnection.
func NewConnectionWithContext(ctx context.Context, factory ConnectionFactory, backoff backoff.Backoff) (*Connection, error) {
	conn, err := factory()
	if err != nil {
		return nil, err
	}

	c := &Connection{
		Connection:        conn,
		connectionFactory: factory,
		alive:             atomic.Bool{},
		backoff:           backoff,
		notifyClose:       make([]chan error, 0),
		notifyReconnect:   make([]chan error, 0),
		mu:                sync.RWMutex{},
	}

	c.alive.Store(true)
	go c.watchReconnect(ctx)

	return c, nil
}

// Connection represents a wrapper over amqp.Connection for use with Server and Publisher.
type Connection struct {
	*amqp.Connection
	connectionFactory func() (*amqp.Connection, error)
	alive             atomic.Bool
	reconnecting      atomic.Bool
	backoff           backoff.Backoff
	notifyClose       []chan error
	notifyReconnect   []chan error
	mu                sync.RWMutex
}

// Close closes underlying *amqp.Connection. It notifies all subscribers about closure with nil error.
// Subscribers can subscribe to Close event with NotifyClose.
func (c *Connection) Close() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("RabbitMQServer closeConn recovered: %v\n", r)
		}
	}()

	if !c.alive.CompareAndSwap(true, false) {
		return
	}

	err := c.Connection.Close()
	if err != nil {
		log.Printf("RabbitMQServer closeConn Close failed: %v\n", err)
	}
}

// NotifyClose registers a new subscriber for closure events.
// When a connection is closed, subscriber will receive a notification with closure error.
func (c *Connection) NotifyClose(ch chan error) chan error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.notifyClose = append(c.notifyClose, ch)
	return ch
}

// NotifyReconnect registers a new subscriber for reconnect events.
// When underlying amqp.Connection is shutdown unexpectedly and reconnection is finished, subscriber will receive a notification with reconnect status.
// Reconnect can be finished with an error if canceled by context.
func (c *Connection) NotifyReconnect(ch chan error) chan error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.notifyReconnect = append(c.notifyReconnect, ch)
	return ch
}

// IsAlive states whether underlying amqp.Connection is closed.
func (c *Connection) IsAlive() bool {
	return c.alive.Load()
}

func (c *Connection) watchReconnect(ctx context.Context) {
	for {
		c.backoff.Reset()

		errClose := <-c.Connection.NotifyClose(make(chan *amqp.Error, 1))
		if errClose != nil {
			c.alive.Store(false)

			log.Println("RabbitMQServer ListenAndServe NotifyClose")

			err := c.attemptReconnect(ctx)
			if err != nil {
				log.Printf("RabbitMQServer ListenAndServe attemptReconnecting failed: %v\n", err)

				c.broadcastReconnect(err)
				return
			}

			log.Println("RabbitMQServer ListenAndServe reconnected successfully")

			c.alive.Store(true)
			c.broadcastReconnect(nil)
			continue
		}

		c.broadcastClose(nil)
		return
	}
}

func (c *Connection) attemptReconnect(ctx context.Context) error {
	c.backoff.Reset()

	if !c.reconnecting.CompareAndSwap(false, true) {
		return nil
	}
	defer c.reconnecting.Store(false)

	for {
		err := c.backoff.Retry(ctx)
		if err != nil {
			return err
		}

		c.Connection, err = c.connectionFactory()
		if err != nil {
			log.Printf("RabbitMQServer attemptReconnecting failed: %s\n", err)
			continue
		}

		return nil
	}
}

func (c *Connection) broadcastClose(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	channels := c.notifyClose
	c.notifyClose = make([]chan error, 0)

	for _, ch := range channels {
		ch <- err
		close(ch)
	}
}

func (c *Connection) broadcastReconnect(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	channels := c.notifyReconnect
	c.notifyReconnect = make([]chan error, 0)

	for _, ch := range channels {
		ch <- err
		close(ch)
	}
}

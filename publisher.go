package rabbitmq

import (
	"context"
	"errors"
	"github.com/MashinIvan/rabbitmq/pkg/backoff"
	"github.com/streadway/amqp"
	"log"
	"sync/atomic"
)

var ErrTooManyFailures = errors.New("too many consecutive failures")

// NewPublisher creates a new *Publisher. Publisher is used to declare exchange and publish messages to this exchange.
func NewPublisher(connection *Connection, exchangeParams ExchangeParams, opts ...PublisherOption) (*Publisher, error) {
	p := &Publisher{
		Conn:           connection,
		exchangeParams: exchangeParams,
	}

	for _, opt := range opts {
		opt(p)
	}

	err := p.prepareChannelAndTransport()
	if err != nil {
		return nil, err
	}

	go p.watchReconnect()

	return p, nil
}

type PublisherOption func(p *Publisher)

// WithRetries makes Publisher.Publish retry on failure with a backoff. consecutiveFailuresBeforeBreak is used as a
// simple circuit breaker. When consecutiveFailuresBeforeBreak is greater than 0 and reached, Publisher.Publish will return ErrTooManyFailures
// and Publisher.Broken will return true.
func WithRetries(backoff backoff.Backoff, consecutiveFailuresBeforeBreak uint32) PublisherOption {
	return func(p *Publisher) {
		p.retryPublish = true
		p.backoff = backoff
		p.consecutiveFailuresAllowed = consecutiveFailuresBeforeBreak
	}
}

// WithQueueDeclaration makes publisher declare a queue on rabbitmq server and bind it to Publisher exchange by bindingKey parameter.
func WithQueueDeclaration(queueParams QueueParams, bindingKey string) PublisherOption {
	return func(p *Publisher) {
		p.declareQueue = true
		p.queueParams = queueParams
		p.bindingKey = bindingKey
	}
}

// Publisher is used to publish messages to single exchange.
// On construction, publisher creates a new channel and declares an exchange. It features an option to declare a queue as well.
// It features an option to retry publish attempts on failures.
type Publisher struct {
	Conn *Connection
	ch   *amqp.Channel

	exchangeParams ExchangeParams
	declareQueue   bool
	queueParams    QueueParams
	bindingKey     string

	retryPublish bool
	backoff      backoff.Backoff

	consecutivePublishFailures atomic.Uint32
	consecutiveFailuresAllowed uint32
}

// Publish sends a message to rabbitmq server.
func (p *Publisher) Publish(ctx context.Context, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if p.Broken() {
		return ErrTooManyFailures
	}

	err := p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
	if err != nil && !p.retryPublish {
		return err
	}

	p.backoff.Reset()

	for {
		err = p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
		if err != nil {
			log.Printf("RabbitMQPublisher Publish failed: %v\n", err)

			err = p.backoff.Retry(ctx)
			if err != nil {
				return err
			}

			p.consecutivePublishFailures.Add(1)
			continue
		}

		p.consecutivePublishFailures.Store(0)
		log.Printf("RabbitMQPublisher Publish to %s %s ok\n", p.exchangeParams.Name, key)
		return nil
	}

	//err := p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
}

// Broken is true, if consecutive publish failures is more than Publisher.consecutiveFailuresAllowed
func (p *Publisher) Broken() bool {
	return p.consecutivePublishFailures.Load() > p.consecutiveFailuresAllowed
}

func (p *Publisher) attemptPublish(key string, mandatory, immediate bool, msg amqp.Publishing) error {
	err := p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
	if err != nil {
		p.consecutivePublishFailures.Add(1)
		return err
	}

	return nil
}

func (p *Publisher) watchReconnect() {
	for {
		select {
		case err := <-p.Conn.NotifyClose(make(chan error)):
			if err != nil {
				log.Printf("RabbitMQPublisher watchReconnect NotifyClose: %v\n", err)
			}

			return
		case err := <-p.Conn.NotifyReconnect(make(chan error)):
			if err != nil {
				log.Printf("RabbitMQPublisher watchReconnect NotifyReconnect: %v\n", err)
				return
			}

			err = p.prepareChannelAndTransport()
			if err != nil {
				log.Printf("RabbitMQPublisher watchReconnect prepareChannelAndTransport failed: %v\n", err)
				return
			}

			log.Println("RabbitMQPublisher watchReconnect reconnected successfully")
			continue
		}
	}
}

func (p *Publisher) prepareChannelAndTransport() error {
	err := p.newChannel()
	if err != nil {
		return err
	}

	err = p.declareAndBind(p.bindingKey)
	if err != nil {
		return err
	}

	return nil
}

func (p *Publisher) newChannel() error {
	var err error
	p.ch, err = p.Conn.Channel()
	return err
}

func (p *Publisher) declareAndBind(bindingKey string) error {
	err := p.ch.ExchangeDeclare(
		p.exchangeParams.Name,
		p.exchangeParams.Kind,
		p.exchangeParams.Durable,
		p.exchangeParams.AutoDelete,
		p.exchangeParams.Internal,
		p.exchangeParams.NoWait,
		p.exchangeParams.Args,
	)
	if err != nil {
		return err
	}

	if p.declareQueue {
		queue, err := p.ch.QueueDeclare(
			p.queueParams.Name,
			p.queueParams.Durable,
			p.queueParams.AutoDelete,
			p.queueParams.Exclusive,
			p.queueParams.NoWait,
			p.queueParams.Args,
		)
		if err != nil {
			return err
		}

		err = p.ch.QueueBind(queue.Name, bindingKey, p.exchangeParams.Name, false, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

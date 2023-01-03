package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
	"log"
	"sync/atomic"
)

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

func WithRetries(backoff Backoff, consecutiveFailuresAllowed uint32) PublisherOption {
	return func(p *Publisher) {
		p.retryPublish = true
		p.backoff = backoff
		p.consecutiveErrorsAllowed = consecutiveFailuresAllowed
	}
}

func WithQueueDeclaration(queueParams QueueParams, bindingKey string) PublisherOption {
	return func(p *Publisher) {
		p.declareQueue = true
		p.queueParams = queueParams
		p.bindingKey = bindingKey
	}
}

type Publisher struct {
	Conn *Connection
	ch   *amqp.Channel

	exchangeParams ExchangeParams
	declareQueue   bool
	queueParams    QueueParams
	bindingKey     string

	retryPublish bool
	backoff      Backoff

	consecutiveErrors        atomic.Uint32
	consecutiveErrorsAllowed uint32
}

func (p *Publisher) Publish(ctx context.Context, key string, mandatory, immediate bool, msg amqp.Publishing) error {
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

			p.consecutiveErrors.Add(1)
			continue
		}

		p.consecutiveErrors.Store(0)
		log.Printf("RabbitMQPublisher Publish to %s %s ok\n", p.exchangeParams.Name, key)
		return nil
	}

	//err := p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
}

// Broken возвращает true в случае, если последовательное количество ошибок Publish > consecutiveFailuresAllowed
func (p *Publisher) Broken() bool {
	return p.consecutiveErrors.Load() > p.consecutiveErrorsAllowed
}

func (p *Publisher) attemptPublish(key string, mandatory, immediate bool, msg amqp.Publishing) error {
	err := p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
	if err != nil {
		p.consecutiveErrors.Add(1)
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

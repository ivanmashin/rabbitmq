package rabbitmq

import (
	"context"
	"errors"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

func NewServer(conn *Connection, router *Router) *Server {
	return &Server{
		Conn:                conn,
		router:              router,
		routerGroupChannels: make(map[*RouterGroup][]*amqp.Channel),
		workersWG:           &sync.WaitGroup{},
		waitCh:              make(chan struct{}),
	}
}

type Server struct {
	Conn *Connection

	router              *Router
	routerGroupChannels map[*RouterGroup][]*amqp.Channel // consumer name to channel

	workersWG *sync.WaitGroup
	waitCh    chan struct{}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	for {
		log.Println("RabbitMQServer ListenAndServe started listening")

		for _, group := range s.router.queuesGroups {
			err := s.bindGroup(group)
			if err != nil {
				return err
			}
		}

		for group, channels := range s.routerGroupChannels {
			for _, ch := range channels {
				deliveryChan, err := ch.Consume(
					group.queueParams.Name,
					group.queueConsumerParams.ConsumerName,
					group.queueConsumerParams.AutoAck,
					false,
					false,
					false,
					group.queueConsumerParams.ConsumerArgs,
				)
				if err != nil {
					return err
				}

				s.workersWG.Add(1)

				go s.worker(ctx, deliveryChan, ch, group)
			}
		}

		// convert wg.Wait() to channel
		go func() { s.workersWG.Wait(); s.waitCh <- struct{}{} }()

		select {
		case err := <-s.Conn.NotifyClose(make(chan error)):
			if err != nil {
				return err
			}
			return nil
		case err := <-s.Conn.NotifyReconnect(make(chan error)):
			if err != nil {
				return err
			}

			s.removeActiveChannelsAfterReconnect()
			continue
		}
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	defer s.Conn.Close()

	for group, channels := range s.routerGroupChannels {
		for _, ch := range channels {
			_ = ch.Cancel(group.queueConsumerParams.ConsumerName, false)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return errors.New("shutdown interrupted")
		case <-s.waitCh:
			return nil
		}
	}
}

func (s *Server) bindGroup(group *RouterGroup) error {
	for i := 0; i < group.workers; i++ {
		ch, err := s.newChannel(group)
		if err != nil {
			return err
		}

		err = ch.ExchangeDeclare(
			group.exchangeParams.Name,
			group.exchangeParams.Kind,
			group.exchangeParams.Durable,
			group.exchangeParams.AutoDelete,
			group.exchangeParams.Internal,
			group.exchangeParams.NoWait,
			group.exchangeParams.Args,
		)
		if err != nil {
			return err
		}

		queue, err := ch.QueueDeclare(
			group.queueParams.Name,
			group.queueParams.Durable,
			group.queueParams.AutoDelete,
			group.queueParams.Exclusive,
			group.queueParams.NoWait,
			group.queueParams.Args,
		)
		if err != nil {
			return err
		}

		err = ch.Qos(group.qos.PrefetchCount, group.qos.PrefetchSize, false)
		if err != nil {
			return err
		}

		for _, bindingKey := range group.bindings {
			err = ch.QueueBind(queue.Name, bindingKey, group.exchangeParams.Name, false, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) newChannel(group *RouterGroup) (*amqp.Channel, error) {
	ch, err := s.Conn.Channel()
	if err != nil {
		return nil, err
	}

	if _, ok := s.routerGroupChannels[group]; !ok {
		s.routerGroupChannels[group] = make([]*amqp.Channel, 0)
	}
	s.routerGroupChannels[group] = append(s.routerGroupChannels[group], ch)

	return ch, nil
}

func (s *Server) removeActiveChannelsAfterReconnect() {
	for group := range s.routerGroupChannels {
		s.routerGroupChannels[group] = make([]*amqp.Channel, 0)
	}
}

func (s *Server) worker(ctx context.Context, deliveryChan <-chan amqp.Delivery, channel *amqp.Channel, group *RouterGroup) {
	defer s.workersWG.Done()

	for delivery := range deliveryChan {
		log.Printf("RabbitMQServerer worker received message with routingKey %s\n", delivery.RoutingKey)

		controllers := group.engine.Route(delivery.RoutingKey)

		if len(controllers) < 1 {
			continue
		}

		for _, controller := range controllers {
			deliveryCtx := NewDeliveryContext(ctx, delivery, channel)
			controller(deliveryCtx)
		}
	}
}

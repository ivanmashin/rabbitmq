package rabbitmq

import (
	"context"
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
)

type DeliveryContext struct {
	context.Context
	Delivery amqp.Delivery
	Channel  *amqp.Channel
}

func NewDeliveryContext(baseCtx context.Context, delivery amqp.Delivery, ch *amqp.Channel) *DeliveryContext {
	return &DeliveryContext{
		Context:  baseCtx,
		Delivery: delivery,
		Channel:  ch,
	}
}

func (c *DeliveryContext) BindJSON(ptr interface{}) error {
	return json.Unmarshal(c.Delivery.Body, ptr)
}

func (c *DeliveryContext) Nack(requeue bool, err error) bool {
	log.Printf("AMQP Consumer %v not acknowledged, error: %v\n", c.Delivery.RoutingKey, err)

	err = c.Delivery.Nack(false, requeue)
	if err != nil {
		return false
	}

	return true
}

func (c *DeliveryContext) Ack() bool {
	err := c.Delivery.Ack(false)
	if err != nil {
		log.Printf("RabbitMQ DeliveryContext Ack failed: %s\n", err)
		return false
	}

	log.Printf("AMQP Consumer %v acknowledged\n", c.Delivery.RoutingKey)
	return true
}

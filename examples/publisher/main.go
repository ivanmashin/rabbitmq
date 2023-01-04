package publisher

import (
	"context"
	"fmt"
	"github.com/MashinIvan/rabbitmq"
	"github.com/MashinIvan/rabbitmq/pkg/backoff"
	"github.com/streadway/amqp"
	"log"
	"os"
)

func main() {
	conn, err := rabbitmq.NewConnection(connFactory, backoff.NewDefaultSigmoidBackoff())
	if err != nil {
		log.Fatal(err)
	}

	runServer(conn)
}

func connFactory() (*amqp.Connection, error) {
	connUrl := fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		os.Getenv("USER"),
		os.Getenv("PASSWORD"),
		os.Getenv("HOST"),
		os.Getenv("PORT"),
	)

	return amqp.Dial(connUrl)
}

func runServer(conn *rabbitmq.Connection) {
	router := provideRouter()
	server := rabbitmq.NewServer(conn, router)

	err := server.ListenAndServe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func provideRouter() *rabbitmq.Router {
	router := rabbitmq.NewRouter()

	router.Group(
		rabbitmq.ExchangeParams{Name: "userEvents", Kind: "Direct"},
		rabbitmq.QueueParams{Name: "users.events"},
		rabbitmq.QualityOfService{},
		rabbitmq.ConsumerParams{},
		rabbitmq.WithRouterEngine(rabbitmq.NewDirectRouterEngine()), // use direct to speed up routing
	).
		Route("users.events.delete", func(ctx *rabbitmq.DeliveryContext) {
			// process delivery
			ctx.Ack()
		}).
		Route("users.events.update", func(ctx *rabbitmq.DeliveryContext) {
			// process delivery
			ctx.Ack()
		})

	// server will bind queue "users.events" to exchange "userEvents" on "users.events.delete" and "users.events.update"
	// binding keys

	router.Group(
		rabbitmq.ExchangeParams{Name: "loggingEvents", Kind: "topic"},
		rabbitmq.QueueParams{Name: "logs"},
		rabbitmq.QualityOfService{},
		rabbitmq.ConsumerParams{},
		rabbitmq.WithRouterEngine(rabbitmq.NewTopicRouterEngine()), // no need to declare, topic is used by default
		rabbitmq.WithNumWorkers(50),
	).
		Route("logs.system.*", func(ctx *rabbitmq.DeliveryContext) {
			// process delivery
			ctx.Ack()
		})

	// server will bind queue "logs" to exchange "loggingEvents" on "logs.system.*" binding keys

	return router
}

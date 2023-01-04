package rabbitmq

import (
	"github.com/streadway/amqp"
	"strings"
)

// NewRouter creates and returns a new Router.
func NewRouter() *Router {
	return &Router{queuesGroups: make([]*RouterGroup, 0)}
}

// Router represents the level of message routing within the queue.
// It registers multiple RouterGroup to be used by Server for exchange and queue declarations and queue bindings.
type Router struct {
	queuesGroups []*RouterGroup
}

// Group accepts generalized transport parameters and constructs RouterGroup.
// It is then used by Server to route messages from defined group queue to controllers based on queue routing.
func (r *Router) Group(exchange ExchangeParams, queue QueueParams, qos QualityOfService, consumer ConsumerParams, opts ...GroupOption) *RouterGroup {
	group := &RouterGroup{
		engine:              NewTopicRouterEngine(),
		exchangeParams:      exchange,
		queueParams:         queue,
		qos:                 qos,
		queueConsumerParams: consumer,
		workers:             1,
		bindings:            make([]string, 0),
	}

	for _, opt := range opts {
		opt(group)
	}

	r.queuesGroups = append(r.queuesGroups, group)
	return group
}

type GroupOption func(g *RouterGroup)

// WithNumWorkers informs Server to use multiple amqp.Channel for that particular routing group.
func WithNumWorkers(workers int) GroupOption {
	return func(g *RouterGroup) {
		g.workers = workers
	}
}

// WithRouterEngine defines what engine will be used to route delivered message to different controllers.
func WithRouterEngine(engine RouterEngine) GroupOption {
	return func(g *RouterGroup) {
		g.engine = engine
	}
}

// RouterGroup generalized consumer's transport, it unites exchange and queue as a single transport line
type RouterGroup struct {
	engine RouterEngine

	exchangeParams      ExchangeParams
	queueParams         QueueParams
	qos                 QualityOfService
	queueConsumerParams ConsumerParams
	workers             int
	bindings            []string
}

// Route registers message routing for one particular queue defined in RouterGroup.
// When receiving messages in a queue, deliveries will be routed to matched controllers based on routingKey parameter.
// routingKey parameter is also used by Server to bind RouterGroup queue to corresponding RouterGroup exchange.
// controllers' chain will be executed for every matched route.
func (g *RouterGroup) Route(routingKey string, controllers ...ControllerFunc) *RouterGroup {
	g.bindings = append(g.bindings, routingKey)
	g.engine.AddBinding(routingKey, controllers...)
	return g
}

// RouterEngine represents message routing logic within a queue.
// When a message is delivered to a queue, RouterEngine is used to route messages to different controllers based on specific pattern.
type RouterEngine interface {

	// AddBinding adds routing from bindingKey to controllers
	// When used by a Server, Server binds RouterGroup queue to RouterGroup exchange on that bindingKey
	AddBinding(bindingKey string, controllers ...ControllerFunc)

	// Route selects all suitable controllers according to the configured routing parameters
	Route(routingKey string) []ControllerFunc
}

// ControllerFunc represents controller type used to process delivered messages.
type ControllerFunc func(ctx *DeliveryContext)

// ExchangeParams generalizes amqp exchange settings
type ExchangeParams struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// QueueParams generalizes amqp queue settings
type QueueParams struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// QualityOfService generalizes amqp qos settings
type QualityOfService struct {
	PrefetchCount int
	PrefetchSize  int
}

// ConsumerParams generalizes amqp consumer settings
type ConsumerParams struct {
	ConsumerName string
	AutoAck      bool
	ConsumerArgs amqp.Table
}

// NewDirectRouterEngine is used for direct message routing as described by amqp0-9-1 protocol.
// For reference on direct routing see https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf.
func NewDirectRouterEngine() RouterEngine {
	return &directRouterEngine{routingMap: make(map[string][]ControllerFunc)}
}

type directRouterEngine struct {
	routingMap map[string][]ControllerFunc
}

func (e *directRouterEngine) AddBinding(bindingKey string, controllers ...ControllerFunc) {
	e.routingMap[bindingKey] = controllers
}

func (e *directRouterEngine) Route(routingKey string) []ControllerFunc {
	return e.routingMap[routingKey]
}

const (
	starNode = "*"
	hashNode = "#"
)

// NewTopicRouterEngine is a topic routing mechanism to route messages as described by amqp0-9-1 protocol.
// For reference on topic routing see https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf.
func NewTopicRouterEngine() RouterEngine {
	return &topicRouterEngine{routingTrieRoot: &topicRouterTrieNode{
		word:     "",
		children: make(map[string]*topicRouterTrieNode),
		bindings: make([]ControllerFunc, 0),
	}}
}

// topicRouterEngine uses prefix tree as suggested by https://blog.rabbitmq.com/posts/2010/09/very-fast-and-scalable-topic-routing-part-1
type topicRouterEngine struct {
	routingTrieRoot *topicRouterTrieNode
}

type topicRouterTrieNode struct {
	word     string
	children map[string]*topicRouterTrieNode
	bindings []ControllerFunc
}

func (e *topicRouterEngine) AddBinding(bindingKey string, controllers ...ControllerFunc) {
	words := strings.Split(bindingKey, ".")

	currentNode := e.routingTrieRoot

	for i, word := range words {
		exists := false
		var childNode *topicRouterTrieNode

		for _, childNode = range currentNode.children {
			if childNode.word == word {
				exists = true
				break
			}
		}

		if !exists {
			childNode = &topicRouterTrieNode{
				word:     word,
				children: make(map[string]*topicRouterTrieNode),
			}

			currentNode.children[word] = childNode
		}

		isLastNode := i == (len(words) - 1)
		if isLastNode {
			childNode.bindings = controllers
		}

		currentNode = childNode
	}
}

func (e *topicRouterEngine) Route(routingKey string) []ControllerFunc {
	words := strings.Split(routingKey, ".")
	result := make([]ControllerFunc, 0)

	e.dfs(e.routingTrieRoot, words, &result)
	return result
}

func (e *topicRouterEngine) dfs(node *topicRouterTrieNode, words []string, result *[]ControllerFunc) {
	if len(words) == 0 {
		childHashNode, ok := node.children[hashNode]
		if ok {
			e.dfs(childHashNode, words, result)
		}

		if len(node.bindings) != 0 {
			*result = append(*result, node.bindings...)
		}
		return
	}

	word, remainingWords := words[0], words[1:]

	childHashNode, ok := node.children[hashNode]
	if ok {
		for i := 0; i <= len(words); i++ {
			e.dfs(childHashNode, words[i:], result)
		}
	}

	childStarNode, ok := node.children[starNode]
	if ok {
		e.dfs(childStarNode, remainingWords, result)
	}

	childWordNode, ok := node.children[word]
	if ok {
		e.dfs(childWordNode, remainingWords, result)
	}
}

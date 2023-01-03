package rabbitmq

import (
	"github.com/streadway/amqp"
	"strings"
)

func NewRouter() *Router {
	return &Router{queuesGroups: make([]*RouterGroup, 0)}
}

type Router struct {
	queuesGroups []*RouterGroup
}

// Group обобщает транспорт consumer`а, объединяя в себе декларацию и параметры exchangeParams и queueParams
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

func WithNumWorkers(workers int) GroupOption {
	return func(g *RouterGroup) {
		g.workers = workers
	}
}

func WithRouterEngine(engine RouterEngine) GroupOption {
	return func(g *RouterGroup) {
		g.engine = engine
	}
}

type RouterGroup struct {
	engine RouterEngine

	exchangeParams      ExchangeParams
	queueParams         QueueParams
	qos                 QualityOfService
	queueConsumerParams ConsumerParams
	workers             int
	bindings            []string
}

// Route регистрирует маршрутизацию сообщений для конкретной очереди в рамках одной RouterGroup
// При чтении из очереди сообщения будут маршрутизироваться в соответствующие контроллеры по параметру RoutingKey
func (g *RouterGroup) Route(routingKey string, controllers ...ControllerFunc) *RouterGroup {
	g.bindings = append(g.bindings, routingKey)
	g.engine.AddBinding(routingKey, controllers...)
	return g
}

type RouterEngine interface {
	// AddBinding добавляет маршрутизацию контроллеров в роутер
	AddBinding(bindingKey string, controllers ...ControllerFunc)
	// Route подбирает все подходящие контроллеры по настроенным параметрам маршрутизации
	Route(routingKey string) []ControllerFunc
}

type ControllerFunc func(ctx *DeliveryContext)

type ExchangeParams struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type QueueParams struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type QualityOfService struct {
	PrefetchCount int
	PrefetchSize  int
}

type ConsumerParams struct {
	ConsumerName string
	AutoAck      bool
	ConsumerArgs amqp.Table
}

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

func NewTopicRouterEngine() RouterEngine {
	return &topicRouterEngine{routingTrieRoot: &topicRouterTrieNode{
		word:     "",
		children: make(map[string]*topicRouterTrieNode),
		bindings: make([]ControllerFunc, 0),
	}}
}

// topicRouterEngine осуществляет роутинг на основе обхода префиксного дерева, см. https://blog.rabbitmq.com/posts/2010/09/very-fast-and-scalable-topic-routing-part-1
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

// dfs реализован согласно https://www.erlang-solutions.com/blog/rabbits-anatomy-understanding-topic-exchanges/
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

package rabbitmq

import (
	"reflect"
	"testing"
)

type routerEngineTestCase struct {
	routingKey string
	triggered  bool
}

func (t *routerEngineTestCase) ControllerFunc(_ *DeliveryContext) {
	t.triggered = true
}

var testCasesDirect = []*routerEngineTestCase{
	{
		routingKey: "one.two.three",
	},
	{
		routingKey: "one.two.four",
	},
	{
		routingKey: "one.two.five",
	},
	{
		routingKey: "one.two.six",
	},
	{
		routingKey: "one.two.three.one",
	},
	{
		routingKey: "one.two.three.two",
	},
	{
		routingKey: "one.two.three.one.one",
	},
	{
		routingKey: "one.two.three.one.two",
	},
}

func resetTestCases(testCases []*routerEngineTestCase) {
	for i, _ := range testCases {
		testCases[i].triggered = false
	}
}

func runControllers(controllers []ControllerFunc) {
	for i, _ := range controllers {
		controllers[i](&DeliveryContext{})
	}
}

func getTriggered(testCases []*routerEngineTestCase) []int {
	result := make([]int, 0)

	for i, _ := range testCases {
		if testCases[i].triggered {
			result = append(result, i)
		}
	}

	return result
}

func TestDirectRouterEngine_Route(t *testing.T) {
	engine := NewDirectRouterEngine()

	for _, testCase := range testCasesDirect {
		engine.AddBinding(testCase.routingKey, testCase.ControllerFunc)
	}

	controllers := engine.Route("one.two.three")
	runControllers(controllers)

	expectedTriggered := []int{0}
	triggered := getTriggered(testCasesDirect)
	if !reflect.DeepEqual(expectedTriggered, triggered) {
		t.Errorf("expected triggered: %v; triggered: %v", expectedTriggered, triggered)
	}

	resetTestCases(testCasesDirect)

	controllers = engine.Route("one.two.four")
	runControllers(controllers)

	expectedTriggered = []int{1}
	triggered = getTriggered(testCasesDirect)
	if !reflect.DeepEqual(expectedTriggered, triggered) {
		t.Errorf("expected triggered: %v; triggered: %v", expectedTriggered, triggered)
	}
}

var testCasesTopic = []*routerEngineTestCase{
	{
		routingKey: "#.air_quality",
	},
	{
		routingKey: "floor_1.#",
	},
	{
		routingKey: "floor_1.bedroom.air_quality.#",
	},

	{
		routingKey: "#",
	},
	{
		routingKey: "test.#",
	},
	{
		routingKey: "#.test",
	},
	{
		routingKey: "test.#.test",
	},
	{
		routingKey: "#.test.#",
	},

	{
		routingKey: "*",
	},
	{
		routingKey: "test.*",
	},
	{
		routingKey: "*.test",
	},
	{
		routingKey: "test.*.test",
	},
	{
		routingKey: "test.*.*.test",
	},

	{
		routingKey: "test",
	},
	{
		routingKey: "test.test",
	},
}

func TestTopicRouterEngine_Route(t *testing.T) {
	engine := NewTopicRouterEngine()

	for _, testCase := range testCasesTopic {
		engine.AddBinding(testCase.routingKey, testCase.ControllerFunc)
	}

	controllers := engine.Route("floor_1.bedroom.air_quality")
	runControllers(controllers)

	expectedTriggered := []int{0, 1, 2, 3}
	triggered := getTriggered(testCasesTopic)
	if !reflect.DeepEqual(expectedTriggered, triggered) {
		t.Errorf("expected triggered: %v; triggered: %v", expectedTriggered, triggered)
	}

	resetTestCases(testCasesTopic)

	controllers = engine.Route("test.cmd.command")
	runControllers(controllers)

	expectedTriggered = []int{3, 4, 7}
	triggered = getTriggered(testCasesTopic)
	if !reflect.DeepEqual(expectedTriggered, triggered) {
		t.Errorf("expected triggered: %v; triggered: %v", expectedTriggered, triggered)
	}
}

func BenchmarkDirectRouterEngine_Route(b *testing.B) {
	engine := NewDirectRouterEngine()
	for _, test := range testCasesDirect {
		engine.AddBinding(test.routingKey, test.ControllerFunc)
	}

	options := []string{
		"one.two.three",
		"one.two.four",
		"one.two.three.one",
		"one.two.three.one.one",
		"one.two.three.one.two",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		engine.Route(options[i%len(options)])
	}
}

func BenchmarkTopicRouterEngine_Route(b *testing.B) {
	engine := NewTopicRouterEngine()
	for _, test := range testCasesDirect {
		engine.AddBinding(test.routingKey, test.ControllerFunc)
	}

	options := []string{
		"one.two.three",
		"one.two.four",
		"one.two.three.one",
		"one.two.three.one.one",
		"one.two.three.one.two",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		engine.Route(options[i%len(options)])
	}
}

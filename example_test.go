package topic_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ardnew/topic"
)

func receive[T any](topics <-chan T) T {
	select {
	case topic := <-topics:
		return topic
	case <-time.After(time.Second):
		panic("timed out waiting for topic")
	}
}

type message struct {
	text string
}

type lazyMessage func() message

func (f lazyMessage) TopicValue() any {
	return f()
}

func ExampleBroker_Publish() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	topics := b.Subscribe[message]().Topics(ctx)

	// The published topic type is automatically inferred from the value.
	b.Publish(message{text: "hello"})
	topic := receive(topics)

	fmt.Println(topic.text)
	// Output:
	// hello
}

func ExampleValuer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	topics := b.Subscribe[message]().Topics(ctx)

	b.Publish(lazyMessage(func() message {
		return message{text: "evaluated when published"}
	}))

	fmt.Println(receive(topics).text)
	// Output:
	// evaluated when published
}

func ExampleWithBufferLen() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	topics := b.Subscribe[int](
		topic.WithBufferLen[int](2),
	).Topics(ctx)

	b.Publish(1)
	b.Publish(2)
	b.Publish(3) // Dropped because the channel is full.

	fmt.Println("capacity:", cap(topics))
	fmt.Println(<-topics)
	fmt.Println(<-topics)
	// Output:
	// capacity: 2
	// 1
	// 2
}

type start struct {
	name string
}

type finish struct {
	success bool
}

func ExampleBroker_Subscribe_multipleTopics() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	startTopics := b.Subscribe[start]().Topics(ctx)
	finishTopics := b.Subscribe[finish]().Topics(ctx)

	// Each published topic type is automatically inferred from its value.
	b.Publish(start{name: "compile"})
	b.Publish(finish{success: true})

	fmt.Println(receive(startTopics).name)
	fmt.Println(receive(finishTopics).success)
	// Output:
	// compile
	// true
}

func ExampleBroker_Subscribe_wildcard() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	topics := b.Subscribe[any]().Topics(ctx)

	// Each published topic type is automatically inferred from its value.
	b.Publish(42)
	b.Publish("disk nearly full")

	for range 2 {
		topic := receive(topics)
		fmt.Printf("%T: %v\n", topic, topic)
	}
	// Output:
	// int: 42
	// string: disk nearly full
}

func ExampleBroker_Subscribe_interface() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	topics := b.Subscribe[io.Reader]().Topics(ctx)

	// The published topic type is automatically inferred as *strings.Reader,
	// a concrete type that implements io.Reader.
	b.Publish(strings.NewReader("concrete reader"))
	topic := receive(topics)
	data, err := io.ReadAll(topic)

	fmt.Println(string(data))
	fmt.Println(err)
	// Output:
	// concrete reader
	// <nil>
}

type text string

func ExampleReceiver_From_filter() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	topics := b.
		Subscribe[text]().
		From(func(topic text) (text, bool) {
			return topic, strings.Contains(string(topic), "check")
		}).
		Topics(ctx)

	b.Publish(text("ignored"))
	b.Publish(text("check this"))

	fmt.Println(receive(topics))
	// Output:
	// check this
}

func ExampleReceiver_From_assignableAndConverted() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b topic.Broker
	// The destination type is selected by Subscribe; From infers its source
	// type from the converter.
	topics := b.
		Subscribe[text]().
		From(func(topic message) (text, bool) {
			return text(strings.ToUpper(topic.text)), true
		}).
		Topics(ctx)

	// A text value is directly assignable to the subscribed topic.
	b.Publish(text("direct"))
	// A message value uses the subscriber's explicit From conversion.
	b.Publish(message{text: "converted"})

	for range 2 {
		fmt.Println(receive(topics))
	}
	// Output:
	// direct
	// CONVERTED
}

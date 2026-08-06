package log_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ardnew/topic"
	"github.com/ardnew/topic/log"
)

type message struct {
	text string
}

func ExampleWrap() {
	var b topic.Broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topics := b.Subscribe[log.Record[message]]().Topics(ctx)

	b.Publish(log.Wrap(slog.LevelInfo, message{text: "ready"}))
	record := <-topics
	fmt.Println(record.Topic.text, record.Level, !record.Time.IsZero(), record.Frame.Line > 0)

	// Output: ready INFO true true
}

func ExampleAtLeast() {
	var b topic.Broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// AtLeast needs an explicit T because it cannot be inferred from its result.
	topics := b.
		Subscribe[log.Record[message]]().
		From(log.AtLeast[message](slog.LevelInfo)).
		Topics(ctx)

	b.Publish(log.Wrap(slog.LevelDebug, message{text: "filtered"}))
	b.Publish(log.Wrap(slog.LevelInfo, message{text: "delivered"}))
	fmt.Println((<-topics).Topic.text)

	// Output: delivered
}

func ExampleUnwrap() {
	var b topic.Broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topics := b.
		Subscribe[message]().
		From(log.Unwrap[message]).
		Topics(ctx)

	b.Publish(log.Wrap(slog.LevelInfo, message{text: "ready"}))
	fmt.Println((<-topics).text)

	// Output: ready
}

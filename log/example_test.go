package log_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

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

func ExampleWithWrap() {
	var b topic.Broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Subscribe infers log.Record[message] from the option.
	topics := b.Subscribe(log.WithWrap[message](slog.LevelInfo)).Topics(ctx)

	// Existing records below Info are filtered. A plain message is wrapped at
	// Info with metadata captured at this Publish call.
	b.Publish(log.Wrap(slog.LevelDebug, message{text: "filtered"}))
	_, file, line, _ := runtime.Caller(0)
	b.Publish(message{text: "ready"})
	record := <-topics
	fmt.Println(
		record.Topic.text,
		record.Level,
		!record.Time.IsZero(),
		record.Frame.File == file && record.Frame.Line == line+1,
	)

	// Output: ready INFO true true
}

func ExampleNew() {
	// T is inferred as message from the topic argument.
	record := log.New(
		slog.LevelInfo,
		"request complete",
		message{text: "served"},
		slog.Group("http", slog.Int("status", 200)),
	)

	fmt.Println(record.Message, record.Topic.text, record.Attrs[0].Key)
	// Output: request complete served http
}

func ExampleRecord_Slog() {
	record := log.New(
		slog.LevelWarn,
		"request delayed",
		message{text: "retrying"},
		slog.Duration("delay", 2*time.Second),
	)
	slogRecord := record.Slog()

	fmt.Println(slogRecord.Message, slogRecord.Level, slogRecord.NumAttrs())
	// Output: request delayed WARN 1
}

func ExampleRecord_Handle() {
	var output bytes.Buffer
	handler := slog.NewTextHandler(&output, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}).WithGroup("app")
	record := log.New(
		slog.LevelInfo,
		"request complete",
		message{text: "served"},
		slog.Group("http", slog.Int("status", 200)),
	)

	if err := record.Handle(context.Background(), handler); err != nil {
		panic(err)
	}
	fmt.Print(output.String())
	// Output: level=INFO msg="request complete" app.http.status=200
}

func ExampleAtLeast() {
	var b topic.Broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Use AtLeast directly when composing a receiver manually. WithWrap provides
	// the shorter form for the common Record subscription.
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

# topic

[![Go Reference](https://pkg.go.dev/badge/github.com/ardnew/topic.svg)](https://pkg.go.dev/github.com/ardnew/topic)

`topic` is an in-memory type-safe pub-sub broker using Go types as topics.

Routing uses Go's type system, so subscribing to a specific type will receive 
only values of that type.
Subscribing to an interface type receives all published values that implement 
that interface; e.g., subscribing to `any` receives all published values.

To subscribe to topics of unrelated types, use `Receiver.From` to convert and 
filter values.

## Usage

The zero value of `Broker` is ready to use.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

var b topic.Broker
topics := b.Subscribe[Event]().Topics(ctx)

b.Publish(Event{Message: "ready"})
event := <-topics
```

Subscribe before publishing. Cancel the context to close the subscription.
Subscriptions can also receive interface types, or convert and filter values
with `Receiver.From`.

`Receiver.From` maps another published type to the subscribed type. Its boolean
result decides whether to deliver the value:

```go
topics := b.
	Subscribe[MyEvent]().
	From(func(ev pkg.SomeEvent) (MyEvent, bool) {
		return MyEvent{Message: ev.Message}, ev.Ready
	}).
	Topics(ctx)

b.Publish(pkg.SomeEvent{Message: "ready", Ready: true})
event := <-topics
```

Delivery is non-blocking. Each channel holds 64 values by default. A value is
dropped when its channel is full. Set another size when subscribing:

```go
topics := b.Subscribe[Event](topic.WithBufferLen[Event](128)).Topics(ctx)
```

A size of `0` makes an unbuffered channel. Negative sizes panic.

## Logging records

The `topic/log` package wraps a topic with a `log/slog` level, timestamp, and
source location. Records use the same broker:

```go
records := b.
	Subscribe[log.Record[Event]]().
	From(log.AtLeast[Event](slog.LevelInfo)).
	Topics(ctx)

b.Publish(log.Wrap(slog.LevelInfo, Event{Message: "ready"}))
record := <-records
```

Use `log.Unwrap` with `Receiver.From` to receive a wrapped topic as its original
type.

## Performance

Publishing allocates no memory in the steady-state benchmarks. Cost grows with
the number of subscribers.

| Case | Time | Allocations |
| --- | ---: | ---: |
| No subscribers | 1.06 ns | 0 |
| One direct subscriber | 34 ns | 0 |
| Four direct subscribers | 115 ns | 0 |
| Sixteen direct subscribers | 409 ns | 0 |

These results used Go 1.27rc2 on an Apple M3 Pro. Run the benchmarks with:

```sh
go test -run '^$' -bench . -benchmem ./...
```

# topic

[![Go Reference](https://pkg.go.dev/badge/github.com/ardnew/topic.svg)](https://pkg.go.dev/github.com/ardnew/topic)

`topic` is an in-memory type-safe pub-sub broker using Go types as topics.

It uses no reflection or serialization and achieves high throughput with
zero heap allocations in most cases.

Routing uses Go's type system.
Subscribing to a specific type will receive only values of that type.
Subscribing to an interface type receives all published values whose type
satisfies that interface;
e.g., subscribing to `any` receives all published values.

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

Implement `topic.Valuer` to defer construction until publication. `Publish`
calls `TopicValue` at most once, and only while the broker has subscribers:

```go
type lazyEvent func() Event

func (f lazyEvent) TopicValue() any { return f() }

b.Publish(lazyEvent(func() Event {
	return Event{Message: expensiveMessage()}
}))
```

`Receiver.From` maps another published type to the subscribed type. Its boolean
result decides whether to deliver the value:

```go
// Subscribe to MyEvent (inferred by the signature of From).
topics := b.
	Subscribe().
	// Define how to convert values published as pkg.SomeEvent to MyEvent.
	From(func(ev pkg.SomeEvent) (MyEvent, bool) {
		return MyEvent{Message: ev.Message}, ev.Ready
	}).
	Topics(ctx)

// This event will be published to both pkg.SomeEvent and MyEvent subscribers.
b.Publish(pkg.SomeEvent{Message: "ready", Ready: true})
event := <-topics
```

Delivery is non-blocking. Each channel holds 64 values by default. A value is
dropped when its channel is full. Set another size when subscribing:

```go
topics := b.
	Subscribe(topic.WithBufferLen[Event](128)).
	Topics(ctx)
```

A size of `0` makes an unbuffered channel. Negative sizes panic.

## Logging records

The `topic/log` package wraps a topic with a `log/slog` level, message,
attributes, timestamp, and source location. Records use the same broker:

```go
records := b.
	Subscribe(log.WithWrap[Event](slog.LevelInfo)).
	Topics(ctx)

b.Publish(Event{Message: "ready"})
record := <-records
```

`WithWrap` also accepts records created with `log.Wrap` or `log.New`, filters
them at the configured minimum level, and preserves accepted records unchanged.
Use `log.New` when a record needs an explicit level, message, and attributes.
Use `log.Unwrap` with `Receiver.From` to receive a wrapped topic as its original
type. Use `Record.Handle` to send a record to any standard handler while
preserving handler-level filtering, groups, and lazy `slog.LogValuer` values:

```go
handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
})
if err := record.Handle(ctx, handler); err != nil {
	return err
}
```

## Performance

The steady-state benchmarks activate subscriptions before timing begins. Times,
bytes, and allocations are per published topic; subscription setup is reported
separately. Values below are medians of three runs using Go 1.27rc2 on
Linux/amd64 with an AMD Ryzen Threadripper 1950X.

### Routing and delivery

Exact routing, same-type adapters, and typed conversion allocate no memory.
Fallback routing of a concrete value through `any` boxes the value, which is
visible in the unmatched and interface cases.

| Case                       | Time/op | Topics/s | B/op | Allocs/op |
| :------------------------- | ------: | -------: | ---: | --------: |
| No subscribers             | 2.26 ns |   442.3M |    0 |         0 |
| Unmatched subscriber type  | 82.5 ns |    12.1M |   16 |         1 |
| Rejected by adapter        | 36.5 ns |    27.4M |    0 |         0 |
| Exact `T` to `T` delivery  | 89.7 ns |    11.1M |    0 |         0 |
| Concrete to interface      |  175 ns |     5.7M |   16 |         1 |
| Converted with `From`      | 87.3 ns |    11.5M |    0 |         0 |
| Accepted filter with `From` | 89.2 ns |    11.2M |    0 |         0 |

### Payload size

| Payload | Time/op | Throughput | Topics/s | B/op | Allocs/op |
| ------: | ------: | ---------: | -------: | ---: | --------: |
|    16 B | 89.6 ns |   179 MB/s |    11.2M |    0 |         0 |
|   1 KiB |  267 ns |  3.83 GB/s |     3.7M |    0 |         0 |

### Fanout

Time/op covers publishing one 16-byte topic and receiving it from every
subscriber. Delivery throughput increases as the fixed routing work is shared
across subscribers.

| Subscribers | Time/topic | Time/delivery | Deliveries/s | B/op | Allocs/op |
| ----------: | ---------: | ------------: | -----------: | ---: | --------: |
|           1 |    90.3 ns |       90.3 ns |        11.1M |    0 |         0 |
|           4 |     244 ns |       61.1 ns |        16.4M |    0 |         0 |
|          16 |     971 ns |       60.7 ns |        16.5M |    0 |         0 |

### Logging records

Metadata capture, level filtering, and automatic wrapping with `log.WithWrap`
remain allocation-free. `log.New` allocates when attributes are supplied
because it copies the slice so the record owns its attributes.

| Case                                | Time/op | B/op | Allocs/op |
| :---------------------------------- | ------: | ---: | --------: |
| `Wrap`, no subscribers              |  458 ns |    0 |         0 |
| Wrapped record, filtered            |  515 ns |    0 |         0 |
| Wrapped record, delivered           |  584 ns |    0 |         0 |
| Plain `T`, automatically wrapped    | 1.43 µs |    0 |         0 |

### Subscription lifecycle

Creating and cancelling a subscription allocates its channel, sender,
registration state, and cancellation goroutine. These setup costs are not part
of the steady-state publication results above.

| Channel capacity | Time/op | Subscriptions/s | B/op | Allocs/op |
| ---------------: | ------: | --------------: | ---: | --------: |
| 64 (default)     | 4.35 µs |            230k | 1600 |        13 |
| 100              | 4.87 µs |            205k | 2240 |        13 |

The same allocation results were reproduced on an Apple M3 Pro. Absolute
timings vary with the CPU, operating system, Go version, and system load.
Reproduce the complete benchmark suite with:

```sh
go test -run '^$' -bench . -benchmem -count=3 ./...
```

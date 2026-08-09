# topic

[![Go Reference](https://pkg.go.dev/badge/github.com/ardnew/topic.svg)](https://pkg.go.dev/github.com/ardnew/topic)

`topic` is an in-memory type-safe pub-sub broker using Go types as topics.
It has no reflection, serialization, metadata, or event wrappers.

Routing follows ordinary Go assignability. A subscriber to a concrete type
receives that type, while a subscriber to an interface receives every
published concrete value that implements it. A subscription to `any` receives
every published value.

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
Subscriptions can also convert and filter values with `Receiver.From`.

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

// The published value is delivered to matching concrete and interface topics.
b.Publish(pkg.SomeEvent{Message: "ready", Ready: true})
event := <-topics
```

Interface subscriptions do not require publishers to name the interface:

```go
readers := b.Subscribe[io.Reader]().Topics(ctx)
b.Publish(strings.NewReader("ready"))
reader := <-readers
```

Delivery is non-blocking. Each channel holds 64 values by default. A value is
dropped when its channel is full. Set another size when subscribing:

```go
topics := b.
	Subscribe(topic.WithBufferLen[Event](128)).
	Topics(ctx)
```

A size of `0` makes an unbuffered channel. Negative sizes panic.

## Performance

The steady-state benchmarks activate subscriptions before timing begins and
report allocations per published topic. Exact-type, pointer-to-interface,
pre-boxed interface, filtered, converted, and exact fanout dispatch remain
allocation-free. An indirect value entering assignability fallback is boxed
once per publication and that box is shared by every interface subscriber.
Without reflection, the same single box may be required to determine that an
indirect value does not satisfy a non-exact subscription.

Subscription setup is outside the steady-state guarantee because it allocates
the channel and registration state.

Run the complete suite with:

```sh
go test -run '^$' -bench . -benchmem -count=3 ./...
```

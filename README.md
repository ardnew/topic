# topic

[![Go Reference](https://pkg.go.dev/badge/github.com/ardnew/topic.svg)](https://pkg.go.dev/github.com/ardnew/topic)

`topic` is an in-memory generic publish-subscribe broker. Go types are the
topics: publishers send ordinary values, and subscribers choose the types they
want to receive. The broker uses no reflection or serialization.

The zero value of `Broker` is ready for concurrent use.

## Direct subscriptions

Create a receiver for a type, activate it with `Topics`, then publish values of
that type:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

type Event struct {
	Message string
}

var broker topic.Broker
events := broker.Subscribe[Event]().Topics(ctx)

broker.Publish(Event{Message: "ready"})
event := <-events
```

Subscribe before publishing. `Topics` returns a new subscription channel each
time it is called. Cancelling the context removes that subscription and closes
its channel.

## Interface subscriptions

Routing follows ordinary Go type assertions. A subscription to an interface
receives values of every published concrete type that implements it. Publishers
do not need to name the interface:

```go
readers := broker.Subscribe[io.Reader]().Topics(ctx)

broker.Publish(strings.NewReader("ready")) // Published as *strings.Reader.
reader := <-readers                         // Received as io.Reader.
```

A subscription to `any` receives every published value. The values are not
wrapped or otherwise changed.

## Mapping and filtering

`Receiver.From` adds a mapping from a source type to the subscribed type. The
callback can convert the value, filter it, or do both:

```go
type WireEvent struct {
	Message string
	Ready   bool
}

type Event struct {
	Message string
}

events := broker.
	Subscribe[Event]().
	From(func(event WireEvent) (Event, bool) {
		return Event{Message: event.Message}, event.Ready
	}).
	Topics(ctx)

broker.Publish(WireEvent{Message: "ready", Ready: true})
event := <-events
```

Mappings are checked in the order they were added. The first mapping whose
source type accepts the published value decides whether that value is
delivered. If no mapping matches, the broker tries a direct type assertion to
the subscribed type. A mapping from a type to itself therefore acts as a filter.

## Buffering and delivery

Delivery is non-blocking and isolated per subscriber. Every subscription has a
buffer of `DefaultBufferLen` values (64 by default). If one subscriber's
channel cannot accept a value, that subscriber drops the value without delaying
publishers or other subscribers.

Set a different capacity when subscribing:

```go
events := broker.Subscribe(
	topic.WithBufferLen[Event](128),
).Topics(ctx)
```

A capacity of zero creates an unbuffered channel, so delivery succeeds only
while a receiver is already waiting. Negative capacities panic.

## Allocation behavior

Steady-state publication uses a typed fast path when the published and
subscribed types are identical.

| Publication path                                    | Heap allocations |
| :-------------------------------------------------- | ---------------: |
| No subscribers                                      |                0 |
| Identical published and subscribed type             |                0 |
| Pointer value delivered to an interface             |                0 |
| Value already held in an interface                  |                0 |
| Published type matches the first `From` source type |                0 |
| Indirect value checked for interface assignment     |                1 |

The indirect-value box is created at most once per publication and shared by
all compatible interface subscribers. Because routing deliberately avoids
reflection, the same box may be needed to determine that an indirect value is
not assignable to a non-identical subscription.

Subscription setup is outside the steady-state guarantee: creating a channel,
registration state, and cancellation goroutine allocates.

Run the tests and benchmarks with:

```sh
go test ./...
go test -run '^$' -bench . -benchmem -count=3 ./...
```

> [!NOTE]
> OpenAI Codex contributed to this project as an AI agent.

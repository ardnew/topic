# topic [![Go Reference](https://pkg.go.dev/badge/github.com/ardnew/topic.svg)](https://pkg.go.dev/github.com/ardnew/topic) [![CI](https://github.com/ardnew/topic/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ardnew/topic/actions/workflows/ci.yml)

### Efficient in-process publish-subscribe broker for Go in which _**types are the topics**_

Publishers send ordinary values, and subscribers choose the types they want to receive.
No names, keys, identifiers, schemas, or registries: a Go type _is_ the topic.

```go
import "github.com/ardnew/topic"

type Tick struct{ N int }

var b topic.Broker            // zero value is ready to use

ch, cancel := b.Subscribe[Tick]()
defer cancel()

b.Publish(Tick{N: 1})         // topic is inferred from the value
fmt.Println((<-ch).N)         // 1
```

Internally, subscriber routing is highly-optimized and features no reflection, serialization, code generation, or dependencies.

**Publishing a value is _allocation-free_ [in nearly all cases](#performance)**.

> [!IMPORTANT]
> This module uses [generic methods], which sets the minimum supported toolchain version at Go 1.27.
>
> All source code is pure Go and standard library-only.

## API

```go
type Broker struct{ ... }

func (b *Broker) Publish[T any](T)
func (b *Broker) Subscribe[T any](...Option[T]) (<-chan T, func())

type Option[T any] struct{ ... }

func Buffer[T any](int) Option[T]
func From[Pub, Sub any](func(Sub) (Pub, bool)) Option[Pub]
```

Messages are published to and subscribed from a zero-valued `Broker`.

Both `Publish` and `Subscribe` are parameterized by a topic `T`,
but `Subscribe` defines the shape and constraints of delivery via `Option[T]` arguments.

## Message delivery

Delivery is **best effort** and **lossy**, by design.
Each subscription has its own channel and capacity;
if it cannot accept a value immediately,
that value is dropped for that subscription
and publication continues for everyone else.

A slow or abandoned subscriber never blocks a publisher.
Nothing is queued, retried, or persisted.

### Matching multiple topics

Wildcard semantics are achieved using Go interfaces.
Subscribing to an interface type receives every published value that implements it;
publishers do not need to name the interface.
Subscribing to `any` receives every value published with that broker.

```go
// Subscribe to all messages whose type implements the error interface.
errs, cancel := b.Subscribe(topic.Buffer[error](16))
```

### Message shaping and filtering

Stricter than [wildcards](#wildcards), `From` subscribes to an additional source type and converts it, statically.
The same function can also be used to filter which values are delivered.

```go
// Create a subscription with topic Fahrenheit.
ch, cancel := b.Subscribe(
    // Convert and deliver all Celsius messages as Fahrenheit.
    topic.From(func(c Celsius) (Fahrenheit, bool) { return Fahrenheit(c*9/5 + 32), true }),
    // Drop delivery of any messages reporting negative Fahrenheit.
    topic.From(func(f Fahrenheit) (Fahrenheit, bool) { return f, f >= 0 }),
)
```

## Performance

Publication is lock-free with respect to the broker.

Publication allocates nothing on a broker whose subscriptions all name concrete types.

Median of ten runs with `go test -run XXX -bench . -benchtime 500000x -count 10 -benchmem`
on an AMD Ryzen Threadripper 1950X, Go 1.27.0:

| Path                                             | Time   | Allocations |
| ------------------------------------------------ | ------ | ----------- |
| no subscriber                                    | 4.8 ns | 0           |
| unmatched topic                                  | 7.5 ns | 0           |
| routing only (saturated subscriber)              | 30 ns  | 0           |
| direct delivery, identical type                  | 97 ns  | 0           |
| pointer to interface subscription                | 109 ns | 0           |
| interface value to interface subscription        | 112 ns | 0           |
| directly matched transformation                  | 99 ns  | 0           |
| directly matched filter (rejecting)              | 20 ns  | 0           |
| small non-pointer value to an `any` subscription | 104 ns | 0           |
| non-pointer value to an `any` subscription       | 143 ns | 1           |
| saturated publication from 32 goroutines         | 43 ns  | 0           |

The single allocation is the one documented exception:

- A subscription to an interface type will receive the value in an interface.
- Boxing a value in an interface costs one allocation per publication, _unless_ the value is:
  - pointer-shaped, or
  - already held in an interface, or
  - zero-sized, or
  - a pointer-free value of 1, 2, 4, or 8 bytes with `uint8`-representable bit pattern; for example:
    - `float32(0)`, `true`, `'x'` are free, but
    - `-1`, `float32(1)`, `[3]byte{}` are not.

  > [!NOTE]
  > This is an optimization of the Go runtime and not this module.

**In either case, the interface conversion is always done once per publication regardless of the number of subscribers, and the allocation is never done if there are no subscribers.**

## Validation

```
gofmt -l .
go vet ./...
go test ./...
go test -race -count=2 ./...
go test -run XXX -bench . ./...
```

## Makefile

Common targets, all writing to `dist/`:

```
make test          # run the suite
make race          # run it under the race detector
make cover report  # coverage profile, then its HTML
make bench         # run the benchmarks
make flame         # CPU flame graph in the browser
make stat          # compare benchmarks against a saved baseline
make debug-test    # open the suite in dlv
make clean
```

Any target can be narrowed with `RUN`, `BENCH`, `PKG`, `COUNT`, or `TIME`:

```
make race RUN=TestDirectDelivery COUNT=5
make flame BENCH=BenchmarkPublishParallel
```

## Credits

> [!NOTE]
> OpenAI Codex and Claude Opus contributed to this project as adversarial AI agents.

[generic methods]: https://go.dev/issue/77273

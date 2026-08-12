# Minimal in-process publish-subscribe broker for Go in which _**types are the topics**_.

[![Go Reference](https://pkg.go.dev/badge/github.com/ardnew/topic.svg)](https://pkg.go.dev/github.com/ardnew/topic)

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

> [!NOTE]
> This module uses [generic methods], which sets the minimum supported toolchain version at Go 1.27.
>
> All source code is pure Go and standard library-only.

## API

```go
type Broker struct{ ... }

func (b *Broker) Publish[T any](v T)
func (b *Broker) Subscribe[T any](opts ...Option[T]) (<-chan T, func())

type Option[T any] struct{ ... }

func Buffer[T any](n int) Option[T]
func From[Pub, Sub any](f func(Sub) (Pub, bool)) Option[Pub]
```

Wildcard semantics are achieved using Go interfaces.
Subscribing to an interface type receives every published value that implements it;
publishers do not need to name the interface.
Subscribing to `any` receives every value published with that broker.

```go
errs, cancel := b.Subscribe(topic.Buffer[error](16))
```

Stricter than wildcards, `From` subscribes to an additional source type and converts it, statically.
The same function can also be used to filter which values are delivered.

```go
ch, cancel := b.Subscribe(
    topic.From(func(c Celsius) (Fahrenheit, bool) { return Fahrenheit(c*9/5 + 32), true }),
    topic.From(func(f Fahrenheit) (Fahrenheit, bool) { return f, f > 0 }),
)
```

## Delivery semantics

Delivery is **best effort** and **lossy**, by design. Each subscription has its own channel and capacity;
if it cannot accept a value immediately, that value is dropped for that subscription and publication continues for everyone else.
A slow or abandoned subscriber never blocks a publisher. Nothing is queued, retried, or persisted.

Cancelling closes the subscription's channel, and after it returns nothing is ever sent on that channel again.

## Performance

Publication is lock-free with respect to the broker.

Publication allocates nothing on a broker whose subscriptions all name concrete types.

Measured with `go test -run XXX -bench . -benchtime 500000x` on an AMD Ryzen Threadripper 1950X, Go 1.27rc2:

| Path                                             | Time   | Allocations |
| ------------------------------------------------ | ------ | ----------- |
| no subscriber                                    | 4.8 ns | 0           |
| unmatched topic                                  | 6.4 ns | 0           |
| routing only (saturated subscriber)              | 32 ns  | 0           |
| direct delivery, identical type                  | 57 ns  | 0           |
| pointer to interface subscription                | 71 ns  | 0           |
| interface value to interface subscription        | 116 ns | 0           |
| directly matched transformation                  | 79 ns  | 0           |
| directly matched filter (rejecting)              | 21 ns  | 0           |
| small non-pointer value to an `any` subscription | 62 ns  | 0           |
| non-pointer value to an `any` subscription       | 140 ns | 1           |
| saturated publication from 32 goroutines         | 47 ns  | 0           |

The single allocation is the one documented exception:
A subscription to an interface type will receive the value in an interface.
Boxing a value in an interface costs one allocation per publication,
_unless_ the value is pointer-shaped, already held in an interface, zero-sized,
or a pointer-free value of 1/2/4/8 bytes with `uint8`-representable bit pattern
(e.g., `float32(0)`, `true`, `'x'` are free; `-1`, `float32(1)`, `[3]byte{}` are not).
This is an optimization of the Go runtime and not this module.

In either case, the interface conversion is always done once per publication regardless of how many subscribers match.

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

> [!INFO]
> OpenAI Codex and Claude Opus contributed to this project as adversarial AI agents.

[generic methods]: https://go.dev/issue/77273

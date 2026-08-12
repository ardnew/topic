package topic

// An Option configures a subscription for values of type T.
//
// The zero Option has no effect. See [Buffer] and [From].
type Option[T any] struct {
	apply func(*config[T])
}

// config accumulates the options given to Subscribe.
type config[T any] struct {
	buffer  int
	sources []source[T]
}

// Buffer sets the capacity of a subscription's channel.
//
// A capacity of zero makes the subscription unbuffered, so a value reaches it
// only if a receiver is already waiting; anything else is dropped. A negative
// capacity is treated as zero. Without this option the capacity is one, the
// smallest buffer that lets a publication survive a consumer that is not
// currently blocked in a receive. Given more than once, the last one wins.
//
// The type argument cannot be inferred from n, so it is written explicitly:
//
//	ch, cancel := b.Subscribe(topic.Buffer[Tick](64))
func Buffer[T any](n int) Option[T] {
	return Option[T]{apply: func(c *config[T]) {
		c.buffer = max(n, 0)
	}}
}

// From declares that a subscription to type Sub also accepts publications of
// type Pub, converting each one with f.
//
// f returns the value to deliver and whether to deliver it: returning false
// drops that value for this subscription, so the same option expresses
// filtering, transformation, or both. Both type parameters are inferred from f.
//
//	// transform
//	b.Subscribe(topic.From(func(c Celsius) (Fahrenheit, bool) {
//		return Fahrenheit(c*9/5 + 32), true
//	}))
//
//	// filter
//	b.Subscribe(topic.From(func(t Tick) (Tick, bool) {
//		return t, t.N%2 == 0
//	}))
//
// The option may be given more than once to declare several source types. A
// subscription considers its sources in the order given, followed by its own
// type, and the first source that matches a publication decides its outcome;
// no later source sees it. Declaring the same source type twice is allowed and
// the later declaration is unreachable.
//
// f runs on the goroutine that published the value, before the value is
// offered to the subscription's channel, so it should be cheap and must not
// block.
func From[Pub, Sub any](f func(Sub) (Pub, bool)) Option[Pub] {
	src := newSource(func(send func(Pub)) func(Sub) {
		return func(v Sub) {
			if r, ok := f(v); ok {
				send(r)
			}
		}
	})
	return Option[Pub]{
		apply: func(c *config[Pub]) { c.sources = append(c.sources, src) },
	}
}

// identity returns the implicit source that accepts publications of the
// subscription's own type unchanged. Its sink is the subscription's send
// function itself, so the most common delivery path has no conversion step.
func identity[T any]() source[T] {
	return newSource(func(send func(T)) func(T) { return send })
}

// A source is one acceptance rule of a subscription: a source type Src, known
// only to the closures below, and a way to build the two runtime forms of the
// rule once the subscription's send function exists.
type source[T any] struct {
	key   any  // keyOf[Src]()
	iface bool // Src is an interface type
	bind  func(send func(T)) bound
}

// A bound source is a source attached to a live subscription. install adds it
// to a routing snapshot as a typed sink; offer matches an already-boxed value
// against it. A subscription uses exactly one of the two.
type bound struct {
	install func(*builder)
	offer   func(key, x any) bool
}

// newSource builds a source for source type Src.
// sink turns the subscription's send function into the typed sink for Src,
// which is where the caller's conversion runs.
func newSource[Src, T any](sink func(send func(T)) func(Src)) source[T] {
	var (
		key    = keyOf[Src]()
		iface  = isIface[Src]()
		anySrc = isAny[Src]()
	)
	return source[T]{
		key:   key,
		iface: iface,
		bind: func(send func(T)) bound {
			accept := sink(send)
			return bound{
				install: func(b *builder) { b.add(accept) },
				offer: func(k, x any) bool {
					// A concrete source matches only a publication of
					// exactly that type, so it is settled by key alone and
					// never asserts. An interface source also matches a
					// value that implements it, and an any source matches
					// everything, including a nil interface value, which no
					// assertion can accept.
					if k != key && !iface {
						return false
					}
					v, ok := x.(Src)
					if k != key && !ok && !anySrc {
						return false
					}
					accept(v)
					return true
				},
			}
		},
	}
}

// keyOf returns a comparable identity for the type T. Two results are equal
// exactly when their type arguments are identical. The value is a nil pointer,
// so building it allocates nothing.
func keyOf[T any]() any { return (*T)(nil) }

// isIface reports whether T is an interface type: the zero value of an
// interface type, and of no other type, holds no dynamic type.
func isIface[T any]() bool {
	var zero T
	return any(zero) == nil
}

// isAny reports whether T is exactly any.
func isAny[T any]() bool {
	_, ok := keyOf[T]().(*any)
	return ok
}

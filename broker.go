package topic

import (
	"slices"
	"sync"
	"sync/atomic"
)

// A Broker routes published values to subscriptions by Go type.
//
// The zero Broker is ready to use. Brokers are independent: a value published
// to one is never observed through another. A Broker must not be copied after
// first use.
type Broker struct {
	mu   sync.Mutex // guards subs and snapshot replacement
	subs []*subscription
	snap atomic.Pointer[snapshot]
}

// snapshot is an immutable routing table. Publication reads it without locks.
type snapshot struct {
	// entries holds one fanout per concrete topic type, keyed by keyOf, and
	// is scanned linearly. A hash map is not used: every type key has a nil
	// data word, so all of them land in one collision chain and a map is
	// several times slower than the scan at every size (see DESIGN.md).
	entries []entry
	// offers holds one entry per subscription that needs dynamic matching.
	offers []func(key, x any)
}

// entry is a topic type and its typed fanout. fan always holds a *fanout[S]
// whose S is the type identified by key.
type entry struct {
	key any
	fan any
}

// fanout holds the typed sinks registered for a single topic type.
type fanout[T any] struct {
	sinks []func(T)
}

// Publish offers v to every subscription that matches it.
//
// The topic is T, the compile-time type of v. Publishing a value no
// subscription matches is valid and has no effect.
//
// Delivery to each matching subscription is a non-blocking send: if the
// subscription cannot accept the value immediately, the value is dropped for
// that subscription only. Publish never blocks on a subscriber and never
// returns before delivery has been attempted for every subscription
// registered when it began.
func (b *Broker) Publish[T any](v T) {
	s := b.snap.Load()
	if s == nil {
		return
	}

	k := keyOf[T]()
	if f := lookup[T](s, k); f != nil {
		for _, sink := range f.sinks {
			sink(v)
		}
	}

	// Only subscriptions with an interface source need the value in an
	// interface. Converting once keeps the cost independent of fan-out, and
	// skipping it entirely keeps the typed path allocation-free.
	if len(s.offers) > 0 {
		x := any(v)
		for _, offer := range s.offers {
			offer(k, x)
		}
	}
}

// Subscribe registers a subscription for values of type T and returns its
// channel and a cancellation function.
//
// By default the subscription accepts publications whose type is T, or whose
// value implements T when T is an interface type. [From] adds source types it
// also accepts, and [Buffer] sets its capacity; without it the capacity is one.
//
// Cancelling unregisters the subscription and closes the channel, which ends a
// range loop over it. Cancelling is idempotent, and after it returns nothing is
// ever sent on the channel again. A subscription that is never cancelled is
// retained by the broker, so callers should cancel when they stop receiving.
func (b *Broker) Subscribe[T any](opts ...Option[T]) (<-chan T, func()) {
	cfg := config[T]{buffer: 1}
	for _, o := range opts {
		if o.apply != nil {
			o.apply(&cfg)
		}
	}

	// Only the first source declared for a type can ever match, so later ones
	// are dropped here rather than left to shadow each other at run time. The
	// subscription's own type is considered last, unless a source claims it.
	sources := make([]source[T], 0, len(cfg.sources)+1)
	claimed := make(map[any]bool, len(cfg.sources)+1)
	for _, src := range cfg.sources {
		if claimed[src.key] {
			continue
		}
		claimed[src.key] = true
		sources = append(sources, src)
	}
	if !claimed[keyOf[T]()] {
		sources = append(sources, identity[T]())
	}

	ch := make(chan T, cfg.buffer)
	sub := &subscription{stop: func() { close(ch) }}

	// send is the one place a value reaches a consumer. The read lock is what
	// makes "never send after close" true in the presence of concurrent
	// cancel; it guards no blocking operation. A read lock suffices because
	// concurrent sends on a channel are already safe, so senders need to
	// exclude only the close in cancel, not one another.
	send := func(v T) {
		sub.mu.RLock()
		if !sub.closed {
			select {
			case ch <- v:
			default:
			}
		}
		sub.mu.RUnlock()
	}

	for _, src := range sources {
		sub.bounds = append(sub.bounds, src.bind(send))
		sub.dynamic = sub.dynamic || src.iface
	}
	if sub.dynamic {
		bounds := sub.bounds
		sub.offer = func(key, x any) {
			for i := range bounds {
				if bounds[i].offer(key, x) {
					return
				}
			}
		}
	}

	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.rebuild()
	b.mu.Unlock()

	return ch, func() { b.cancel(sub) }
}

// cancel unregisters sub and closes its channel. It is idempotent.
func (b *Broker) cancel(sub *subscription) {
	b.mu.Lock()
	if i := slices.Index(b.subs, sub); i >= 0 {
		b.subs = slices.Delete(b.subs, i, i+1)
		b.rebuild()
	}
	b.mu.Unlock()

	// Unregistering first bounds the window in which a publication that
	// already loaded the old snapshot can still deliver; the lock closes it.
	sub.mu.Lock()
	if !sub.closed {
		sub.closed = true
		sub.stop()
	}
	sub.mu.Unlock()
}

// rebuild replaces the routing snapshot. b.mu must be held.
func (b *Broker) rebuild() {
	var (
		bld  builder
		snap snapshot
	)
	for _, sub := range b.subs {
		if sub.dynamic {
			snap.offers = append(snap.offers, sub.offer)
			continue
		}
		for i := range sub.bounds {
			sub.bounds[i].install(&bld)
		}
	}
	snap.entries = bld.entries
	b.snap.Store(&snap)
}

// subscription is the broker-side state of one Subscribe call.
type subscription struct {
	bounds  []bound
	dynamic bool             // at least one source type is an interface
	offer   func(key, x any) // nil unless dynamic

	mu     sync.RWMutex
	closed bool
	stop   func()
}

// builder accumulates typed fanouts while a snapshot is being rebuilt. Every
// fanout it creates is fresh, so a snapshot already in use is never mutated.
type builder struct {
	entries []entry
}

// add appends sink to the fanout for type S, creating it if needed.
func (b *builder) add[S any](sink func(S)) {
	k := keyOf[S]()
	for i := range b.entries {
		if b.entries[i].key == k {
			f := b.entries[i].fan.(*fanout[S])
			f.sinks = append(f.sinks, sink)
			return
		}
	}
	b.entries = append(b.entries, entry{key: k, fan: &fanout[S]{sinks: []func(S){sink}}})
}

// lookup returns the fanout registered for type T, or nil.
func lookup[T any](s *snapshot, k any) *fanout[T] {
	for i := range s.entries {
		if s.entries[i].key == k {
			return s.entries[i].fan.(*fanout[T])
		}
	}
	return nil
}

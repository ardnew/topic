package topic

import (
	"context"
	"sync"
)

// adapter reports separately whether the source type matched and whether the
// resulting topic should be delivered.
type adapter[T any] func(any) (topic T, matched, accepted bool)

// Receiver configures a subscription for values of topic type T. Build a
// receiver with [Broker.Subscribe], then call [Receiver.Topics]. A [Receiver]
// must not be modified concurrently with [Receiver.Topics].
type Receiver[T any] struct {
	broker    *Broker
	adapters  []adapter[T]
	exact     exactRegistration[T]
	bufferLen int
}

// WithBufferLen sets the channel capacity for a [Receiver] created by
// [Broker.Subscribe]. The default is [DefaultBufferLen]. A length of zero
// creates an unbuffered channel. WithBufferLen panics if length is negative.
func WithBufferLen[T any](length int) Option[Receiver[T]] {
	if length < 0 {
		panic("topic: negative buffer length")
	}
	return func(r *Receiver[T]) { r.bufferLen = length }
}

// From adds a source mapping from published topic type M to receiver topic type
// T. The callback may convert, filter, or do both; its bool determines whether
// the result is delivered. Mappings are checked in registration order before
// ordinary assignability, and the first matching source type owns the decision.
func (r *Receiver[T]) From[M any](fn func(M) (T, bool)) *Receiver[T] {
	// Only the first mapping can always be dispatched solely from its exact
	// source type. Later mappings may be shadowed by an earlier interface source,
	// so they retain the ordinary assignability fallback.
	if len(r.adapters) == 0 {
		r.exact = mappedExactRegistration(fn)
	}
	r.adapters = append(r.adapters, func(value any) (T, bool, bool) {
		topic, ok := value.(M)
		if !ok {
			var zero T
			return zero, false, false
		}
		converted, accepted := fn(topic)
		return converted, true, accepted
	})
	return r
}

// Topics activates r and returns its value channel. The channel closes when ctx
// is cancelled. Delivery is non-blocking; a value is dropped when the channel
// is full.
func (r *Receiver[T]) Topics(ctx context.Context) <-chan T {
	exact := r.exact
	if len(r.adapters) == 0 {
		exact = directExactRegistration[T]()
	}
	return r.broker.topics(
		ctx,
		r.adapters,
		exact,
		r.bufferLen,
	)
}

type typedSender[T any] struct {
	mu       sync.RWMutex
	topics   chan T
	adapters []adapter[T]
	exactKey exactKeyValue
	closed   bool
}

func (s *typedSender[T]) handlesExact(key exactKeyValue) bool {
	return s.exactKey == key
}

func (s *typedSender[T]) send(value any) bool {
	// Rule 1: the first adapter whose source type matches decides whether the
	// message is delivered. This permits adapters from T to T to act as filters.
	for _, adapter := range s.adapters {
		topic, matched, accepted := adapter(value)
		if !matched {
			continue
		}
		if accepted {
			s.deliver(topic)
		}
		return accepted
	}

	// Rule 2: ordinary Go assignability, including concrete-to-interface.
	if topic, ok := value.(T); ok {
		s.deliver(topic)
		return true
	}
	return false
}

func (s *typedSender[T]) deliver(topic T) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.topics <- topic:
	default:
	}
}

func (s *typedSender[T]) close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.topics)
	}
	s.mu.Unlock()
}

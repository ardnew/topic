package topic

import (
	"context"
	"sync"
	"sync/atomic"
)

// DefaultBufferLen is the capacity of each subscriber channel.
// Override it with [WithBufferLen] when calling [Broker.Subscribe].
const DefaultBufferLen = 64

// Option configures a value of type T using the functional options pattern.
type Option[T any] func(*T)

// Valuer provides a topic value at publication time. [Broker.Publish] calls
// TopicValue at most once, and only when the broker has subscribers. The
// dynamic type of the returned value determines which subscribers receive it.
//
// Valuer is useful when constructing a topic is expensive or captures
// time-sensitive information.
type Valuer interface {
	TopicValue() any
}

// Broker is a type-safe pub-sub dispatcher. Its zero value is ready for use.
// A [Broker] must not be copied after first use.
type Broker struct {
	reg   registry
	exact sync.Map
}

// Publish publishes topic. If topic implements [Valuer], Publish calls its
// TopicValue method and publishes the returned value instead.
func (b *Broker) Publish[T any](topic T) {
	snapshot := b.reg.snapshot()
	if snapshot == nil {
		return
	}
	if _, ok := any(topic).(Valuer); ok {
		b.publishValue(snapshot, evaluate(topic))
		return
	}
	b.publish(snapshot, topic)
}

func evaluate[T any](topic T) any {
	return any(topic).(Valuer).TopicValue()
}

func (b *Broker) publish[T any](snapshot *registrySnapshot, topic T) {
	key := exactKey[T]{}
	if route, ok := b.exact.Load(key); ok {
		route.(*exactRoute[T]).publish(topic)
	}

	// Exact subscribers have already received topic without boxing it. Only
	// construct an interface value if an assignability fallback is required.
	var value any
	boxed := false
	for _, s := range *snapshot {
		if s.handlesExact(key) {
			continue
		}
		if !boxed {
			value = topic
			boxed = true
		}
		s.send(value)
	}
}

func (b *Broker) publishValue(snapshot *registrySnapshot, topic any) {
	for _, s := range *snapshot {
		s.send(topic)
	}
}

// Subscribe begins configuring a subscription for topic type T. Options
// configure the returned [Receiver].
func (b *Broker) Subscribe[T any](opts ...Option[Receiver[T]]) *Receiver[T] {
	r := &Receiver[T]{broker: b, bufferLen: DefaultBufferLen}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (b *Broker) topics[T any](
	ctx context.Context,
	adapters []adapter[T],
	exact exactRegistration[T],
	bufferLen int,
) <-chan T {
	topics := make(chan T, bufferLen)
	s := &typedSender[T]{
		topics:   topics,
		adapters: adapters,
		exactKey: exact.key,
	}
	id := b.reg.add(s)
	removeExact := exact.add(b, id, s)
	go func() {
		<-ctx.Done()
		removeExact()
		b.reg.remove(id)
	}()
	return topics
}

// sender abstracts delivery to one typed subscriber channel. All type
// knowledge is captured at [Broker.Subscribe] time.
type sender interface {
	send(any) bool
	handlesExact(exactKeyValue) bool
	close()
}

type registrySnapshot []sender

// registry holds all active subscriber senders keyed by a monotonic ID.
type registry struct {
	mu      sync.Mutex
	senders map[uint64]sender
	next    uint64
	current atomic.Pointer[registrySnapshot]
}

func (r *registry) add(s sender) uint64 {
	r.mu.Lock()
	if r.senders == nil {
		r.senders = make(map[uint64]sender)
	}
	id := r.next
	r.next++
	r.senders[id] = s
	r.storeSnapshot()
	r.mu.Unlock()
	return id
}

func (r *registry) remove(id uint64) {
	r.mu.Lock()
	s, ok := r.senders[id]
	if ok {
		delete(r.senders, id)
		r.storeSnapshot()
	}
	r.mu.Unlock()
	if ok {
		s.close()
	}
}

func (r *registry) storeSnapshot() {
	if len(r.senders) == 0 {
		r.current.Store(nil)
		return
	}
	snapshot := make(registrySnapshot, 0, len(r.senders))
	for _, s := range r.senders {
		snapshot = append(snapshot, s)
	}
	r.current.Store(&snapshot)
}

func (r *registry) snapshot() *registrySnapshot {
	return r.current.Load()
}

package topic

import (
	"sync"
	"sync/atomic"
)

// exactKey identifies one instantiated publication type without reflection.
// Empty values are allocation-free map keys; their dynamic types distinguish
// exactKey[A] from exactKey[B].
type exactKey[T any] struct{}

type exactRegistration[T any] struct {
	key exactKeyValue
	add func(*Broker, uint64, *typedSender[T]) func()
}

// exactKeyValue keeps heterogeneous exact keys in common data structures. The
// concrete values stored here are always comparable exactKey[T] values.
type exactKeyValue = any

type exactRouteSnapshot[T any] []func(T)

// exactRoute is the allocation-free publication path for one source type T.
// Mutations copy the current snapshot; publication only performs an atomic
// load followed by typed function calls.
type exactRoute[T any] struct {
	mu      sync.Mutex
	senders map[uint64]func(T)
	current atomic.Pointer[exactRouteSnapshot[T]]
}

func (r *exactRoute[T]) add(id uint64, send func(T)) {
	r.mu.Lock()
	if r.senders == nil {
		r.senders = make(map[uint64]func(T))
	}
	r.senders[id] = send
	r.storeSnapshot()
	r.mu.Unlock()
}

func (r *exactRoute[T]) remove(id uint64) {
	r.mu.Lock()
	if _, ok := r.senders[id]; ok {
		delete(r.senders, id)
		r.storeSnapshot()
	}
	r.mu.Unlock()
}

func (r *exactRoute[T]) storeSnapshot() {
	if len(r.senders) == 0 {
		r.current.Store(nil)
		return
	}
	snapshot := make(exactRouteSnapshot[T], 0, len(r.senders))
	for _, send := range r.senders {
		snapshot = append(snapshot, send)
	}
	r.current.Store(&snapshot)
}

func (r *exactRoute[T]) publish(topic T) {
	snapshot := r.current.Load()
	if snapshot == nil {
		return
	}
	for _, send := range *snapshot {
		send(topic)
	}
}

func exactRouteFor[T any](b *Broker) *exactRoute[T] {
	key := exactKey[T]{}
	if route, ok := b.exact.Load(key); ok {
		return route.(*exactRoute[T])
	}
	created := new(exactRoute[T])
	actual, _ := b.exact.LoadOrStore(key, created)
	return actual.(*exactRoute[T])
}

func directExactRegistration[T any]() exactRegistration[T] {
	return exactRegistration[T]{
		key: exactKey[T]{},
		add: func(b *Broker, id uint64, sender *typedSender[T]) func() {
			route := exactRouteFor[T](b)
			route.add(id, sender.deliver)
			return func() { route.remove(id) }
		},
	}
}

func mappedExactRegistration[M, T any](
	convert func(M) (T, bool),
) exactRegistration[T] {
	return exactRegistration[T]{
		key: exactKey[M]{},
		add: func(b *Broker, id uint64, sender *typedSender[T]) func() {
			route := exactRouteFor[M](b)
			route.add(id, func(source M) {
				if topic, accepted := convert(source); accepted {
					sender.deliver(topic)
				}
			})
			return func() { route.remove(id) }
		},
	}
}

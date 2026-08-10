// Package topic provides an in-process publish-subscribe broker in which Go
// types are the topics.
//
// A publisher offers an ordinary Go value; a subscriber selects a Go type and
// receives values of that type on a channel. Nothing else identifies a topic:
// there are no names, keys, identifiers, schemas, or registries to declare or
// maintain, and the package uses no reflection, serialization, or code
// generation.
//
// # Basic use
//
// The zero Broker is ready to use.
//
//	type Tick struct{ N int }
//
//	var b topic.Broker
//
//	ch, cancel := b.Subscribe[Tick]()
//	defer cancel()
//
//	b.Publish(Tick{N: 1})
//	fmt.Println((<-ch).N) // 1
//
// The topic of a publication is the compile-time type of the published value,
// and the topic of a subscription is its result type. Publishing through an
// interface variable therefore publishes on that interface type, and a
// subscription naming a concrete type does not see it.
//
// # Matching
//
// A subscription accepts a publication when one of its source types matches
// it. The source types are, in order, those declared with [From] followed by
// the subscription's own type. A source type S matches a publication of type T
// when S and T are identical, when S is an interface type that the published
// value implements, or when S is any.
//
// An interface source is matched against the published value itself, so it
// sees the value's dynamic type. Publishing through an interface variable can
// therefore reach a subscription to a different interface that the underlying
// value happens to implement.
//
// The first matching source decides the outcome; no later source is consulted.
// A source declared with [From] delivers the value it returns when it reports
// true and drops the value when it reports false, which is how filtering is
// expressed. Because the subscription's own type is considered last, direct
// matching always remains available.
//
// A nil value of an interface type carries no dynamic type, so it reaches only
// subscriptions whose type is identical to the published interface type, and
// subscriptions to any.
//
// # Delivery and loss
//
// Delivery is a non-blocking send performed while Publish runs. Each
// subscription has its own channel and its own capacity, set with [Buffer]. If
// a subscription's buffer is full, or it is unbuffered and no receiver is
// waiting, the value is dropped for that subscription and publication
// continues for the others. A slow or abandoned subscriber therefore never
// blocks a publisher or another subscriber.
//
// Delivery is best effort and confined to one process. Nothing is queued,
// retried, persisted, or sent over a network, and no delivery is guaranteed.
// Values one subscription accepts from sequential, non-overlapping
// publications are received in publication order; ordering between concurrent
// publications is unspecified.
//
// # Lifecycle
//
// The function returned by [Broker.Subscribe] unregisters the subscription and
// closes its channel, which ends a range loop over it. Values already accepted
// remain receivable. It is idempotent, and after it returns nothing is ever
// sent on that channel again.
//
// # Concurrency
//
// A Broker is safe for concurrent publication, subscription, cancellation, and
// delivery. A Broker must not be copied after first use.
//
// # Allocation
//
// Publication allocates nothing, whatever the value's type and however many
// subscriptions match, on a broker whose subscriptions all name concrete
// types. A subscription to an interface type or to any needs the value in an
// interface, which costs one allocation per publication unless the value is
// pointer-shaped or already held in an interface. That conversion is done once
// and shared by every such subscription, so the cost never depends on how many
// subscribers match, but it does apply to every publication on that broker.
package topic

// Package topic is an in-memory type-safe pub-sub broker.
//
// Topics are Go types, not values. Publishing a value of type M delivers an
// unwrapped value to every subscriber whose topic type T is satisfied by M.
//
// Routing uses ordinary Go type assertions without reflection, metadata,
// wrappers, or serialization. Subscribing to an interface type T receives all
// published values whose concrete types implement T; subscribing to [any]
// receives all published values:
//
//	readers := b.Subscribe[io.Reader]().Topics(ctx)
//	b.Publish(strings.NewReader("ready"))
//
//	var b topic.Broker
//	topics := b.Subscribe[MyEvent]().Topics(ctx)
//	b.Publish(MyEvent{Message: "ready"})
//	topic := <-topics
//	fmt.Println(topic.Message)
//
// Subscribers may adapt or filter topics using [Receiver.From]. Its callback
// converts a source value to the receiver's topic type and decides whether the
// converted value is delivered.
//
// Subscriber channels have capacity [DefaultBufferLen]. Pass [WithBufferLen]
// to [Broker.Subscribe] to override the capacity for that receiver. Delivery
// remains non-blocking: a topic is dropped when the channel cannot accept it.
// A buffer length of zero creates an unbuffered channel, so delivery succeeds
// only while a receiver is ready.
//
// Exact-type dispatch, pointer-to-interface dispatch, and pre-boxed interface
// dispatch perform no heap allocations. An indirect value entering the
// assignability fallback is boxed once per publication and shared across
// fanout. Subscription setup allocates the channel and registration state.
package topic

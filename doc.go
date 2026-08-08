// Package topic is an in-memory type-safe pub-sub broker.
//
// Topics are Go types, not values. Publishing a value of type M delivers an
// unwrapped value to every subscriber whose topic type T is satisfied by M.
//
// Routing uses ordinary type assertions without reflection, metadata, or
// serialization. Subscribing to an interface type T receives all published
// values of concrete types M that implement T; subscribing to [any] receives
// all published values.
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
// A value implementing [Valuer] is evaluated by [Broker.Publish] only when the
// broker has subscribers. This defers expensive or time-sensitive topic
// construction without requiring a separate publication method.
//
// Subscriber channels have capacity [DefaultBufferLen]. Pass [WithBufferLen]
// to [Broker.Subscribe] to override the capacity for that receiver. Delivery
// remains non-blocking: a topic is dropped when the channel cannot accept it.
// A buffer length of zero creates an unbuffered channel, so delivery succeeds
// only while a receiver is ready.
package topic

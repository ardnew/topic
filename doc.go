// Package topic provides an in-memory generic publish-subscribe broker.
//
// Go types are topics. Publishing a value delivers it unchanged to subscribers
// of the same type and to subscribers of every interface the value implements.
// A subscription to [any] receives every published value. Routing uses ordinary
// Go type assertions without reflection or serialization.
//
//	var broker topic.Broker
//	events := broker.Subscribe[Event]().Topics(ctx)
//	broker.Publish(Event{Message: "ready"})
//	event := <-events
//
// [Receiver.From] adds ordered conversion and filtering rules. The first rule
// whose source type accepts a published value decides whether it is delivered;
// if no rule matches, a direct type assertion to the subscribed type is
// attempted.
//
// Delivery is non-blocking and isolated per subscriber. Channels have capacity
// [DefaultBufferLen] unless [WithBufferLen] overrides it. A full channel drops
// the value for that subscriber. Cancelling the context passed to
// [Receiver.Topics] removes the subscription and closes its channel.
//
// Identical-type publication and pointer-to-interface publication perform no
// steady-state heap allocations. Assignability checks for indirect values may
// box the value once; that box is shared across subscriber fanout.
package topic

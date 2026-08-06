// Package log adds opt-in logging records to a topic broker.
//
// A [Record] wraps a typed topic with a log level, timestamp, and publishing
// source. Publish a wrapped topic through [topic.Broker.Publish], filter records
// with [AtLeast] and [topic.Receiver.From], or adapt records back to their
// underlying topics with [Unwrap]. Existing records can be forwarded unchanged
// through [topic.Broker.Publish].
//
// [topic.Broker.Publish]: https://pkg.go.dev/github.com/ardnew/topic#Broker.Publish
// [topic.Receiver.From]: https://pkg.go.dev/github.com/ardnew/topic#Receiver.From
package log

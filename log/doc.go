// Package log adds opt-in logging records to a topic broker.
//
// A [Record] wraps a typed topic with a log level, message, attributes,
// timestamp, and publishing source. [New] constructs structured records, while
// [Wrap] retains the minimal metadata-only form. [WithWrap] configures a
// subscription that receives records and automatically wraps plain topics at a
// default level. Publish records through [topic.Broker.Publish], filter them
// with [AtLeast] and [topic.Receiver.From], adapt them back to their underlying
// topics with [Unwrap], or send them to any standard [slog.Handler] with
// [Record.Handle]. Existing records can be forwarded unchanged through
// [topic.Broker.Publish].
package log

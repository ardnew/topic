package log

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ardnew/topic"
)

// Record wraps a typed topic with a structured log event.
// A Record must not be modified after it is published.
type Record[T any] struct {
	Topic   T
	Level   slog.Level
	Time    time.Time
	Frame   runtime.Frame
	Message string
	Attrs   []slog.Attr
}

// WithWrap returns a [topic.Option] for subscribing to [Record][T] values.
// Published values of type T are automatically wrapped at level with metadata
// captured at the call to [topic.Broker.Publish]. Published [Record][T] values
// at or above level are delivered unchanged; lower-level records are filtered.
//
// Pass the returned option to [topic.Broker.Subscribe]. The option lets
// Subscribe infer its receiver type without spelling Record[T] at the call
// site. Automatic wrapping does not allocate during steady-state publication.
func WithWrap[T any](level slog.Level) topic.Option[topic.Receiver[Record[T]]] {
	var callerSkip atomic.Int32
	return func(r *topic.Receiver[Record[T]]) {
		r.
			From(AtLeast[T](level)).
			From(func(value T) (Record[T], bool) {
				return wrapPublished(level, value, &callerSkip), true
			})
	}
}

func wrapPublished[T any](
	level slog.Level,
	topic T,
	callerSkip *atomic.Int32,
) Record[T] {
	when, frame := capturePublish(callerSkip)
	return Record[T]{Topic: topic, Level: level, Time: when, Frame: frame}
}

// Wrap returns topic wrapped with level and metadata captured at the call site.
func Wrap[T any](level slog.Level, topic T) Record[T] {
	return wrapAndSkip(level, topic, 3)
}

func wrapAndSkip[T any](level slog.Level, topic T, skip int) Record[T] {
	when, frame := capture(skip)
	return Record[T]{Topic: topic, Level: level, Time: when, Frame: frame}
}

// New returns a structured record with metadata captured at the call site.
// New copies attrs, so subsequent changes to the argument slice do not affect
// the record.
func New[T any](
	level slog.Level,
	message string,
	topic T,
	attrs ...slog.Attr,
) Record[T] {
	when, frame := capture(2)
	return Record[T]{
		Topic:   topic,
		Level:   level,
		Time:    when,
		Frame:   frame,
		Message: message,
		Attrs:   append([]slog.Attr(nil), attrs...),
	}
}

// Slog returns a new [slog.Record] containing record's logging metadata and
// attributes. The returned record does not share mutable slog.Record state with
// values returned by other calls.
func (record Record[T]) Slog() slog.Record {
	r := slog.NewRecord(
		record.Time,
		record.Level,
		record.Message,
		record.Frame.PC,
	)
	r.AddAttrs(record.Attrs...)
	return r
}

// Handle sends record to handler when handler is enabled for record's level.
// A nil context is replaced with [context.Background], matching [slog.Logger].
// Handler errors are returned to the caller.
func (record Record[T]) Handle(ctx context.Context, handler slog.Handler) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !handler.Enabled(ctx, record.Level) {
		return nil
	}
	return handler.Handle(ctx, record.Slog())
}

// Unwrap adapts a [Record] back to its topic for use with
// [topic.Receiver.From].
func Unwrap[T any](record Record[T]) (T, bool) {
	return record.Topic, true
}

// AtLeast returns an adapter that accepts records at or above min.
// A nil minimum defaults to [slog.LevelDebug].
func AtLeast[T any](min slog.Leveler) func(Record[T]) (Record[T], bool) {
	if min == nil {
		min = slog.LevelDebug
	}
	return func(record Record[T]) (Record[T], bool) {
		return record, record.Level >= min.Level()
	}
}

// Keep capture as a physical frame so each caller's fixed skip remains stable.
//
//go:noinline
func capture(skip int) (time.Time, runtime.Frame) {
	when := time.Now()
	var pcs [1]uintptr
	if n := runtime.Callers(skip+1, pcs[:]); n == 0 {
		return when, runtime.Frame{}
	}
	return when, frameForReturnPC(pcs[0])
}

// Keep capturePublish as a physical frame while it identifies Publish and its
// caller. Searching by function avoids a compiler- and architecture-sensitive
// stack-depth constant.
//
//go:noinline
func capturePublish(callerSkip *atomic.Int32) (time.Time, runtime.Frame) {
	when := time.Now()
	if skip := callerSkip.Load(); skip != 0 {
		var pcs [1]uintptr
		if n := runtime.Callers(int(skip), pcs[:]); n != 0 {
			return when, frameForReturnPC(pcs[0])
		}
	}

	var pcs [16]uintptr
	n := runtime.Callers(2, pcs[:])
	for i := 0; i+1 < n; i++ {
		returnPC := pcs[i]
		fn := runtime.FuncForPC(returnPC - 1)
		if fn != nil && strings.HasPrefix(
			fn.Name(),
			"github.com/ardnew/topic.(*Broker).Publish",
		) {
			callerSkip.CompareAndSwap(0, int32(i+3))
			return when, frameForReturnPC(pcs[i+1])
		}
	}
	return when, runtime.Frame{}
}

func frameForReturnPC(returnPC uintptr) runtime.Frame {
	// Callers returns the PC immediately after the call instruction. Move it
	// back into the call before resolving its inline-aware function and source.
	pc := returnPC - 1
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return runtime.Frame{PC: pc}
	}
	file, line := fn.FileLine(pc)
	return runtime.Frame{
		PC:       pc,
		Func:     fn,
		Function: fn.Name(),
		File:     file,
		Line:     line,
		Entry:    fn.Entry(),
	}
}

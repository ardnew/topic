package log

import (
	"log/slog"
	"runtime"
	"time"
)

// Record wraps a typed topic with logging metadata.
type Record[T any] struct {
	Topic T
	Level slog.Level
	Time  time.Time
	Frame runtime.Frame
}

// Wrap returns topic wrapped with level and metadata captured at the call site.
func Wrap[T any](level slog.Level, topic T) Record[T] {
	when, frame := capture(2)
	return Record[T]{Topic: topic, Level: level, Time: when, Frame: frame}
}

// Unwrap adapts a [Record] back to its topic for use with
// [topic.Receiver.From].
//
// [topic.Receiver.From]: https://pkg.go.dev/github.com/ardnew/topic#Receiver.From
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

func capture(skip int) (time.Time, runtime.Frame) {
	when := time.Now()
	pc := make([]uintptr, 1)
	if n := runtime.Callers(skip+1, pc); n == 0 {
		return when, runtime.Frame{}
	}
	frame, _ := runtime.CallersFrames(pc).Next()
	return when, frame
}

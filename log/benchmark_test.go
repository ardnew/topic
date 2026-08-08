package log

import (
	"log/slog"
	"testing"

	"github.com/ardnew/topic"
)

func BenchmarkPublish(b *testing.B) {
	b.Run("filtered", func(b *testing.B) {
		var broker topic.Broker
		_ = broker.Subscribe(WithWrap[int](slog.LevelInfo)).Topics(b.Context())
		b.ReportAllocs()
		for b.Loop() {
			broker.Publish(Wrap(slog.LevelDebug, 42))
		}
	})

	b.Run("without_subscribers", func(b *testing.B) {
		var broker topic.Broker
		b.ReportAllocs()
		for b.Loop() {
			broker.Publish(Wrap(slog.LevelInfo, 42))
		}
	})

	b.Run("delivered", func(b *testing.B) {
		var broker topic.Broker
		topics := broker.Subscribe[Record[int]]().Topics(b.Context())
		b.ReportAllocs()
		for b.Loop() {
			broker.Publish(Wrap(slog.LevelInfo, 42))
			<-topics
		}
	})

	b.Run("auto_wrapped", func(b *testing.B) {
		var broker topic.Broker
		topics := broker.Subscribe(WithWrap[int](slog.LevelInfo)).Topics(b.Context())
		b.ReportAllocs()
		for b.Loop() {
			broker.Publish(42)
			<-topics
		}
	})
}

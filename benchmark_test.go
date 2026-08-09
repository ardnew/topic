package topic_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ardnew/topic"
)

type small struct {
	seq     uint64
	enabled bool
}

type large struct {
	data [1024]byte
}

type converted struct {
	seq uint64
}

type marked interface {
	topic()
}

func (small) topic() {}

func BenchmarkPublish(b *testing.B) {
	b.Run("without_subscribers", func(b *testing.B) {
		var broker topic.Broker
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			broker.Publish(small{seq: uint64(i), enabled: true})
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), 0)
	})

	b.Run("unmatched", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[string]().Topics(ctx)
		b.Cleanup(func() {
			cancel()
			for range topics {
			}
		})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			broker.Publish(small{seq: uint64(i), enabled: true})
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), 0)
	})

	b.Run("filtered", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[small]().
			From(func(topic small) (small, bool) { return topic, false }).
			Topics(ctx)
		b.Cleanup(func() {
			cancel()
			for range topics {
			}
		})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			broker.Publish(small{seq: uint64(i), enabled: true})
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), 0)
	})
}

func BenchmarkDelivery(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[small]().Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := small{enabled: true}
		b.SetBytes(16)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.seq = uint64(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})

	b.Run("interface_pointer", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[marked]().Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := &small{enabled: true}
		b.SetBytes(16)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.seq = uint64(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})

	b.Run("interface_value", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[marked]().Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := small{enabled: true}
		b.SetBytes(16)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.seq = uint64(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})

	b.Run("converted", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[converted]().
			From(func(topic small) (converted, bool) {
				return converted{seq: topic.seq}, true
			}).
			Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := small{enabled: true}
		b.SetBytes(16)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.seq = uint64(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})

	b.Run("filtered", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[small]().
			From(func(topic small) (small, bool) {
				return topic, topic.enabled
			}).
			Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := small{enabled: true}
		b.SetBytes(16)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.seq = uint64(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})
}

func BenchmarkDeliveryPayload(b *testing.B) {
	b.Run("16B", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[small]().Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := small{enabled: true}
		b.SetBytes(16)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.seq = uint64(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})
	b.Run("1KiB", func(b *testing.B) {
		var broker topic.Broker
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[large]().Topics(ctx)
		defer closeTopics(cancel, topics)

		topic := large{}
		b.SetBytes(1024)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			topic.data[0] = byte(i)
			broker.Publish(topic)
			<-topics
		}
		b.StopTimer()
		reportMetrics(b, int64(b.N), int64(b.N))
	})
}

func BenchmarkDeliveryBufferLen(b *testing.B) {
	for _, bufferLen := range []int{1, 10, topic.DefaultBufferLen, 100, 1000} {
		b.Run(fmt.Sprintf("len=%d", bufferLen), func(b *testing.B) {
			var broker topic.Broker
			ctx, cancel := context.WithCancel(b.Context())
			topics := broker.Subscribe[small](
				topic.WithBufferLen[small](bufferLen),
			).Topics(ctx)
			defer closeTopics(cancel, topics)

			topic := small{enabled: true}
			b.SetBytes(16)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				topic.seq = uint64(i)
				broker.Publish(topic)
				<-topics
			}
			b.StopTimer()
			b.ReportMetric(float64(cap(topics)), "channel-cap")
			reportMetrics(b, int64(b.N), int64(b.N))
		})
	}
}

func BenchmarkPublishFullBuffer(b *testing.B) {
	for _, bufferLen := range []int{0, 1, topic.DefaultBufferLen, 1000} {
		b.Run(fmt.Sprintf("len=%d", bufferLen), func(b *testing.B) {
			var broker topic.Broker
			ctx, cancel := context.WithCancel(b.Context())
			topics := broker.Subscribe[small](
				topic.WithBufferLen[small](bufferLen),
			).Topics(ctx)
			defer closeTopics(cancel, topics)

			for i := 0; i < bufferLen; i++ {
				broker.Publish(small{seq: uint64(i), enabled: true})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				broker.Publish(small{seq: uint64(i), enabled: true})
			}
			b.StopTimer()
			b.ReportMetric(float64(cap(topics)), "channel-cap")
			reportMetrics(b, int64(b.N), 0)
		})
	}
}

func BenchmarkDeliveryFanout(b *testing.B) {
	for _, subscribers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("subscribers=%d", subscribers), func(b *testing.B) {
			fanoutDelivery(b, subscribers)
		})
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	publishParallel(b)
}

func publishParallel(b *testing.B) {
	b.Helper()
	var broker topic.Broker
	ctx, cancel := context.WithCancel(b.Context())
	topics := broker.Subscribe[small]().Topics(ctx)

	var delivered atomic.Int64
	var drain sync.WaitGroup
	ready := make(chan struct{})
	drain.Add(1)
	go func() {
		defer drain.Done()
		close(ready)
		for range topics {
			delivered.Add(1)
		}
	}()
	<-ready

	b.SetBytes(16)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var seq uint64
		for pb.Next() {
			broker.Publish(small{seq: seq, enabled: true})
			seq++
		}
	})
	b.StopTimer()

	cancel()
	drain.Wait()
	reportMetrics(b, int64(b.N), delivered.Load())
}

func BenchmarkSubscriptionLifecycle(b *testing.B) {
	var broker topic.Broker
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[small]().Topics(ctx)
		cancel()
		for range topics {
		}
	}
	b.StopTimer()

	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "subscriptions/s")
	}
}

func BenchmarkSubscriptionLifecycleWithBufferLen(b *testing.B) {
	var broker topic.Broker
	option := topic.WithBufferLen[small](100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(b.Context())
		topics := broker.Subscribe[small](option).Topics(ctx)
		cancel()
		for range topics {
		}
	}
	b.StopTimer()

	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "subscriptions/s")
	}
}

func fanoutDelivery(b *testing.B, subscribers int) {
	b.Helper()
	var broker topic.Broker
	ctx, cancel := context.WithCancel(b.Context())
	topics := make([]<-chan small, subscribers)
	for i := range topics {
		topics[i] = broker.Subscribe[small]().Topics(ctx)
	}
	defer func() {
		cancel()
		for _, ts := range topics {
			for range ts {
			}
		}
	}()

	batchSize := cap(topics[0])
	if batchSize < 1 {
		batchSize = 1
	}
	topic := small{enabled: true}
	b.SetBytes(16)
	b.ReportAllocs()
	b.ResetTimer()
	for published := 0; published < b.N; {
		batch := min(batchSize, b.N-published)
		for i := 0; i < batch; i++ {
			topic.seq = uint64(published + i)
			broker.Publish(topic)
		}
		for _, ts := range topics {
			for i := 0; i < batch; i++ {
				<-ts
			}
		}
		published += batch
	}
	b.StopTimer()

	deliveries := int64(b.N) * int64(subscribers)
	reportMetrics(b, int64(b.N), deliveries)
}

func closeTopics[T any](cancel context.CancelFunc, topics <-chan T) {
	cancel()
	for range topics {
	}
}

func reportMetrics(b *testing.B, topics, deliveries int64) {
	b.Helper()
	elapsed := b.Elapsed()
	if elapsed <= 0 {
		return
	}
	seconds := elapsed.Seconds()
	b.ReportMetric(float64(topics)/seconds, "topics/s")
	b.ReportMetric(float64(deliveries)/seconds, "deliveries/s")
	if topics > 0 {
		b.ReportMetric(float64(deliveries)/float64(topics), "deliveries/topic")
	}
	if deliveries > 0 {
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(deliveries), "ns/delivery")
	}
}

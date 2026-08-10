package topic_test

import (
	"strconv"
	"testing"

	"github.com/ardnew/topic"
)

// Benchmarks drain their subscriptions on the publishing goroutine rather than
// from a concurrent consumer, so that what is measured is the broker's routing
// and delivery cost rather than the Go scheduler. The parallel benchmarks are
// the deliberate exception.

// drainBuffer is large enough that draining is rare and its cost amortizes.
const drainBuffer = 1024

// take empties ch.
func take[T any](ch <-chan T) {
	for len(ch) > 0 {
		<-ch
	}
}

// BenchmarkPublishNoSubscriber measures publication to a broker with no
// subscriptions at all.
func BenchmarkPublishNoSubscriber(b *testing.B) {
	var br topic.Broker
	v := payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
	}
}

// BenchmarkPublishUnmatchedTopic measures publication of a type no
// subscription matches, on a broker that has other subscriptions.
func BenchmarkPublishUnmatchedTopic(b *testing.B) {
	var br topic.Broker
	_, cancel := br.Subscribe(topic.Buffer[reading](1))
	defer cancel()
	v := payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
	}
}

// BenchmarkPublishDirect measures delivery where the published and
// subscription types are identical.
func BenchmarkPublishDirect(b *testing.B) {
	var br topic.Broker
	ch, cancel := br.Subscribe(topic.Buffer[payload](drainBuffer))
	defer cancel()
	v := payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
		if len(ch) == cap(ch) {
			take(ch)
		}
	}
}

// BenchmarkPublishInterfacePointer measures delivery of a pointer value to a
// satisfied interface subscription.
func BenchmarkPublishInterfacePointer(b *testing.B) {
	var br topic.Broker
	ch, cancel := br.Subscribe(topic.Buffer[meter](drainBuffer))
	defer cancel()
	p := &payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(p)
		if len(ch) == cap(ch) {
			take(ch)
		}
	}
}

// BenchmarkPublishInterfaceValue measures delivery of a value already held in
// an interface to that interface subscription.
func BenchmarkPublishInterfaceValue(b *testing.B) {
	var br topic.Broker
	ch, cancel := br.Subscribe(topic.Buffer[meter](drainBuffer))
	defer cancel()
	var m meter = &payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(m)
		if len(ch) == cap(ch) {
			take(ch)
		}
	}
}

// BenchmarkPublishAny measures the one publication path that allocates: a
// value that is neither pointer-shaped nor already an interface, reaching an
// any subscription.
func BenchmarkPublishAny(b *testing.B) {
	var br topic.Broker
	ch, cancel := br.Subscribe(topic.Buffer[any](drainBuffer))
	defer cancel()
	v := payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
		if len(ch) == cap(ch) {
			take(ch)
		}
	}
}

// BenchmarkPublishTransformed measures a directly matched typed conversion.
func BenchmarkPublishTransformed(b *testing.B) {
	var br topic.Broker
	ch, cancel := br.Subscribe(
		topic.Buffer[reading](drainBuffer),
		topic.From(func(p payload) (reading, bool) { return reading{v: float64(p.a)}, true }),
	)
	defer cancel()
	v := payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
		if len(ch) == cap(ch) {
			take(ch)
		}
	}
}

// BenchmarkPublishFiltered measures a directly matched filter that rejects
// every value, so nothing is enqueued.
func BenchmarkPublishFiltered(b *testing.B) {
	var br topic.Broker
	_, cancel := br.Subscribe(
		topic.Buffer[payload](drainBuffer),
		topic.From(func(p payload) (payload, bool) { return p, false }),
	)
	defer cancel()
	v := payload{a: 1}

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
	}
}

// BenchmarkPublishFanOut measures fan-out to n subscriptions of the identical
// type.
func BenchmarkPublishFanOut(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run("n="+strconv.Itoa(subs), func(b *testing.B) {
			var br topic.Broker
			chans := make([]<-chan payload, subs)
			for i := range chans {
				ch, cancel := br.Subscribe(topic.Buffer[payload](drainBuffer))
				defer cancel()
				chans[i] = ch
			}
			v := payload{a: 1}

			b.ReportAllocs()
			for b.Loop() {
				br.Publish(v)
				if len(chans[0]) == cap(chans[0]) {
					for _, ch := range chans {
						take(ch)
					}
				}
			}
		})
	}
}

// BenchmarkPublishFullCapacity measures publication to a saturated
// subscription: the value is dropped and no channel operation completes.
func BenchmarkPublishFullCapacity(b *testing.B) {
	var br topic.Broker
	_, cancel := br.Subscribe(topic.Buffer[payload](1))
	defer cancel()
	v := payload{a: 1}
	br.Publish(v)

	b.ReportAllocs()
	for b.Loop() {
		br.Publish(v)
	}
}

// BenchmarkPublishParallel measures concurrent publication to one broker whose
// subscription is drained by a separate consumer.
func BenchmarkPublishParallel(b *testing.B) {
	var br topic.Broker
	ch, cancel := br.Subscribe(topic.Buffer[payload](drainBuffer))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	v := payload{a: 1}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Publish(v)
		}
	})

	b.StopTimer()
	cancel()
	<-done
}

// BenchmarkPublishParallelSaturated measures the routing and delivery path
// under contention with no consumer at all, so no channel handoff occurs.
func BenchmarkPublishParallelSaturated(b *testing.B) {
	var br topic.Broker
	_, cancel := br.Subscribe(topic.Buffer[payload](1))
	defer cancel()
	v := payload{a: 1}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Publish(v)
		}
	})
}

// BenchmarkPublishTopics measures routing as the number of distinct topic
// types registered on one broker grows. The measured subscription is
// registered last, so the scan must walk the whole table, and it is saturated,
// so the numbers isolate routing from delivery.
func BenchmarkPublishTopics(b *testing.B) {
	for _, topics := range []int{1, 8, 16, 32} {
		b.Run("n="+strconv.Itoa(topics), func(b *testing.B) {
			var br topic.Broker
			for _, register := range registrars[:topics-1] {
				defer register(&br)()
			}
			_, cancel := br.Subscribe(topic.Buffer[payload](1))
			defer cancel()
			v := payload{a: 1}
			br.Publish(v)

			b.ReportAllocs()
			for b.Loop() {
				br.Publish(v)
			}
		})
	}
}

// BenchmarkPublishTopicsMiss measures the worst case for scanning: a
// publication that matches nothing, so the whole table is walked.
func BenchmarkPublishTopicsMiss(b *testing.B) {
	for _, topics := range []int{1, 8, 16, 32} {
		b.Run("n="+strconv.Itoa(topics), func(b *testing.B) {
			var br topic.Broker
			for _, register := range registrars[:topics] {
				defer register(&br)()
			}
			v := payload{a: 1} // no subscription is registered for payload

			b.ReportAllocs()
			for b.Loop() {
				br.Publish(v)
			}
		})
	}
}

// BenchmarkRebuild measures registration cost as the number of distinct topic
// types grows, which is deliberately outside the steady-state budget.
func BenchmarkRebuild(b *testing.B) {
	for _, topics := range []int{8, 32} {
		b.Run("n="+strconv.Itoa(topics), func(b *testing.B) {
			var br topic.Broker
			for _, register := range registrars[:topics] {
				defer register(&br)()
			}

			b.ReportAllocs()
			for b.Loop() {
				_, cancel := br.Subscribe[payload]()
				cancel()
			}
		})
	}
}

// BenchmarkSubscribe measures registration and cancellation, which are
// deliberately outside the steady-state allocation budget.
func BenchmarkSubscribe(b *testing.B) {
	var br topic.Broker

	b.ReportAllocs()
	for b.Loop() {
		_, cancel := br.Subscribe[payload]()
		cancel()
	}
}

// registrar returns a function that subscribes to T and returns its cancel.
func registrar[T any]() func(*topic.Broker) func() {
	return func(b *topic.Broker) func() {
		_, cancel := b.Subscribe(topic.Buffer[T](1))
		return cancel
	}
}

// registrars supplies distinct topic types for BenchmarkPublishTopics.
var registrars = []func(*topic.Broker) func(){
	registrar[int8](), registrar[int16](), registrar[int32](), registrar[int64](),
	registrar[uint8](), registrar[uint16](), registrar[uint32](), registrar[uint64](),
	registrar[float32](), registrar[float64](), registrar[complex64](), registrar[complex128](),
	registrar[bool](), registrar[string](), registrar[uintptr](), registrar[int](),
	registrar[[1]int](), registrar[[2]int](), registrar[[3]int](), registrar[[4]int](),
	registrar[[5]int](), registrar[[6]int](), registrar[[7]int](), registrar[[8]int](),
	registrar[[1]bool](), registrar[[2]bool](), registrar[[3]bool](), registrar[[4]bool](),
	registrar[[5]bool](), registrar[[6]bool](), registrar[[7]bool](), registrar[[8]bool](),
}

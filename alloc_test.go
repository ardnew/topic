package topic_test

import (
	"testing"

	"github.com/ardnew/topic"
)

// payload is deliberately larger than a word and contains no pointers, so
// placing it in an interface would allocate. Any path below that reports zero
// allocations therefore never boxed the value.
type payload struct{ a, b, c, d int64 }

type reading struct{ v float64 }

type meter interface{ read() float64 }

func (p *payload) read() float64 { return float64(p.a) }

// allocs reports allocations per publication, draining the subscription each
// round so that saturation never hides work.
func allocs(t *testing.T, publish func()) float64 {
	t.Helper()
	return testing.AllocsPerRun(1000, publish)
}

func TestAllocsNoSubscribers(t *testing.T) {
	var b topic.Broker
	v := payload{a: 1}

	if got := allocs(t, func() { b.Publish(v) }); got != 0 {
		t.Errorf("publish with no subscribers: %v allocs, want 0", got)
	}
}

func TestAllocsAfterAllSubscriptionsCancelled(t *testing.T) {
	var b topic.Broker
	_, cancel := b.Subscribe[payload]()
	cancel()
	v := payload{a: 1}

	if got := allocs(t, func() { b.Publish(v) }); got != 0 {
		t.Errorf("publish after cancellation: %v allocs, want 0", got)
	}
}

func TestAllocsDirectIdenticalType(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[payload](1))
	defer cancel()
	v := payload{a: 1}

	got := allocs(t, func() {
		b.Publish(v)
		<-ch
	})
	if got != 0 {
		t.Errorf("direct delivery: %v allocs, want 0", got)
	}
}

func TestAllocsFanOutIdenticalType(t *testing.T) {
	const subs = 16
	var b topic.Broker
	chans := make([]<-chan payload, subs)
	for i := range chans {
		ch, cancel := b.Subscribe(topic.Buffer[payload](1))
		defer cancel()
		chans[i] = ch
	}
	v := payload{a: 1}

	got := allocs(t, func() {
		b.Publish(v)
		for _, ch := range chans {
			<-ch
		}
	})
	if got != 0 {
		t.Errorf("fan-out to %d subscriptions: %v allocs, want 0", subs, got)
	}
}

func TestAllocsPointerToInterfaceSubscription(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[meter](1))
	defer cancel()
	p := &payload{a: 1}

	got := allocs(t, func() {
		b.Publish(p)
		<-ch
	})
	if got != 0 {
		t.Errorf("pointer to interface subscription: %v allocs, want 0", got)
	}
}

func TestAllocsInterfaceValueToInterfaceSubscription(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[meter](1))
	defer cancel()
	var m meter = &payload{a: 1}

	got := allocs(t, func() {
		b.Publish(m)
		<-ch
	})
	if got != 0 {
		t.Errorf("interface value to interface subscription: %v allocs, want 0", got)
	}
}

func TestAllocsErrorValueToErrorSubscription(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[error](1))
	defer cancel()
	var err error = &failure{code: 1}

	got := allocs(t, func() {
		b.Publish(err)
		<-ch
	})
	if got != 0 {
		t.Errorf("error value to error subscription: %v allocs, want 0", got)
	}
}

func TestAllocsDirectlyMatchedTransformation(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[reading](1),
		topic.From(func(p payload) (reading, bool) { return reading{v: float64(p.a)}, true }),
	)
	defer cancel()
	v := payload{a: 1}

	got := allocs(t, func() {
		b.Publish(v)
		<-ch
	})
	if got != 0 {
		t.Errorf("directly matched transformation: %v allocs, want 0", got)
	}
}

func TestAllocsDirectlyMatchedFilter(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[payload](1),
		topic.From(func(p payload) (payload, bool) { return p, p.a > 0 }),
	)
	defer cancel()
	pass := payload{a: 1}
	drop := payload{a: -1}

	if got := allocs(t, func() { b.Publish(drop) }); got != 0 {
		t.Errorf("rejected by filter: %v allocs, want 0", got)
	}
	got := allocs(t, func() {
		b.Publish(pass)
		<-ch
	})
	if got != 0 {
		t.Errorf("accepted by filter: %v allocs, want 0", got)
	}
}

func TestAllocsSaturatedSubscription(t *testing.T) {
	var b topic.Broker
	_, cancel := b.Subscribe(topic.Buffer[payload](1))
	defer cancel()
	v := payload{a: 1}
	b.Publish(v) // fill the buffer; every later publication is dropped

	if got := allocs(t, func() { b.Publish(v) }); got != 0 {
		t.Errorf("publish to a saturated subscription: %v allocs, want 0", got)
	}
}

// TestAllocsBoxedPathIsIndependentOfFanOut pins the documented cost of the one
// publication path that does allocate: a value that is neither pointer-shaped
// nor already an interface, offered to interface subscriptions. It must cost
// exactly one allocation no matter how many subscriptions match.
func TestAllocsBoxedPathIsIndependentOfFanOut(t *testing.T) {
	for _, subs := range []int{1, 2, 16} {
		var b topic.Broker
		chans := make([]<-chan any, subs)
		for i := range chans {
			ch, cancel := b.Subscribe(topic.Buffer[any](1))
			defer cancel()
			chans[i] = ch
		}
		v := payload{a: 1}

		got := allocs(t, func() {
			b.Publish(v)
			for _, ch := range chans {
				<-ch
			}
		})
		if got != 1 {
			t.Errorf("%d any subscriptions: %v allocs, want exactly 1", subs, got)
		}
	}
}

// TestAllocsSmallValueToAnySubscription records that the boxing allocation is
// a cost of the value, not of the broker. The runtime keeps the first 256
// integers in read-only storage and boxes any one-, two-, four-, or
// eight-byte pointer-free value whose bits fall in that range by pointing at
// it, so such a publication reaches an interface subscription for free.
//
// This is a property of the Go runtime rather than a promise of this package.
// If it ever stops holding, the performance table in README.md is wrong and
// this test is how that is discovered.
func TestAllocsSmallValueToAnySubscription(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[any](1))
	defer cancel()
	v := tick{n: 1} // one word, no pointers, and 1 < 256

	got := allocs(t, func() {
		b.Publish(v)
		<-ch
	})
	if got != 0 {
		t.Errorf("small value to an any subscription: %v allocs, want 0", got)
	}

	// The same type carrying a number outside that range must box normally,
	// which is what makes the result above a statement about the value.
	big := tick{n: 256}
	if got := allocs(t, func() {
		b.Publish(big)
		<-ch
	}); got != 1 {
		t.Errorf("large value to an any subscription: %v allocs, want exactly 1", got)
	}
}

// TestAllocsMixedWorlds records that the typed path's zero-allocation
// guarantee is a property of the broker, not of the publication alone: a
// single interface subscription anywhere on the broker forces the one
// documented boxing allocation for every publication of a value that is
// neither pointer-shaped nor already an interface. The count stays at one
// however many subscriptions of either kind match.
func TestAllocsMixedWorlds(t *testing.T) {
	for _, n := range []int{1, 4, 16} {
		var b topic.Broker
		typed := make([]<-chan payload, n)
		boxed := make([]<-chan any, n)
		for i := range n {
			ch, cancel := b.Subscribe(topic.Buffer[payload](1))
			defer cancel()
			typed[i] = ch

			anyCh, cancelAny := b.Subscribe(topic.Buffer[any](1))
			defer cancelAny()
			boxed[i] = anyCh
		}
		v := payload{a: 1}

		got := allocs(t, func() {
			b.Publish(v)
			for i := range n {
				<-typed[i]
				<-boxed[i]
			}
		})
		if got != 1 {
			t.Errorf("%d typed and %d any subscriptions: %v allocs, want exactly 1", n, n, got)
		}
	}
}

// TestAllocsDynamicSubscriptionConcreteSource covers the one dynamic-world
// path that must still be free: a pointer publication reaching a concrete
// source of a subscription that also declares an interface source.
func TestAllocsDynamicSubscriptionConcreteSource(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[reading](1),
		topic.From(func(m meter) (reading, bool) { return reading{v: m.read()}, true }),
		topic.From(func(p *payload) (reading, bool) { return reading{v: float64(p.b)}, true }),
	)
	defer cancel()
	p := &payload{b: 2}

	got := allocs(t, func() {
		b.Publish(p)
		<-ch
	})
	if got != 0 {
		t.Errorf("pointer to a dynamic subscription: %v allocs, want 0", got)
	}
}

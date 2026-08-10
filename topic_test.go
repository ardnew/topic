package topic_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ardnew/topic"
)

type tick struct{ n int }

type tock struct{ n int }

type named string

func (n named) String() string { return string(n) }

type failure struct{ code int }

func (f *failure) Error() string { return fmt.Sprintf("failure %d", f.code) }

// drain receives up to n values, or fewer if the channel closes or the timeout
// expires.
func drain[T any](t *testing.T, ch <-chan T, n int) []T {
	t.Helper()
	got := make([]T, 0, n)
	deadline := time.After(2 * time.Second)
	for len(got) < n {
		select {
		case v, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, v)
		case <-deadline:
			t.Fatalf("timed out with %d of %d values", len(got), n)
		}
	}
	return got
}

func TestZeroBrokerPublishNoSubscribers(t *testing.T) {
	var b topic.Broker
	b.Publish(tick{1}) // must not panic
}

func TestDirectDelivery(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe[tick]()
	defer cancel()

	b.Publish(tick{7})

	if got := (<-ch).n; got != 7 {
		t.Fatalf("n = %d, want 7", got)
	}
}

func TestUnrelatedTypesAreNotDelivered(t *testing.T) {
	var b topic.Broker
	ticks, cancelTicks := b.Subscribe[tick]()
	defer cancelTicks()
	tocks, cancelTocks := b.Subscribe[tock]()
	defer cancelTocks()

	b.Publish(tock{3})

	if len(ticks) != 0 {
		t.Errorf("tick subscription received %d values, want 0", len(ticks))
	}
	if got := (<-tocks).n; got != 3 {
		t.Errorf("tock n = %d, want 3", got)
	}
}

func TestFanOut(t *testing.T) {
	const subs = 8
	var b topic.Broker

	chans := make([]<-chan tick, subs)
	for i := range chans {
		ch, cancel := b.Subscribe[tick]()
		defer cancel()
		chans[i] = ch
	}

	b.Publish(tick{42})

	for i, ch := range chans {
		if got := (<-ch).n; got != 42 {
			t.Errorf("subscription %d: n = %d, want 42", i, got)
		}
	}
}

func TestBrokersAreIndependent(t *testing.T) {
	var a, b topic.Broker
	ach, acancel := a.Subscribe[tick]()
	defer acancel()
	bch, bcancel := b.Subscribe[tick]()
	defer bcancel()

	a.Publish(tick{1})

	if got := (<-ach).n; got != 1 {
		t.Errorf("a: n = %d, want 1", got)
	}
	if len(bch) != 0 {
		t.Errorf("b received %d values, want 0", len(bch))
	}
}

func TestSubscriptionsAreIndependent(t *testing.T) {
	var b topic.Broker
	// One subscriber is saturated at capacity 1 and never drained; the other
	// must still receive everything.
	slow, cancelSlow := b.Subscribe(topic.Buffer[tick](1))
	defer cancelSlow()
	fast, cancelFast := b.Subscribe(topic.Buffer[tick](16))
	defer cancelFast()

	for i := range 10 {
		b.Publish(tick{i})
	}

	if len(slow) != 1 {
		t.Errorf("slow subscription holds %d values, want 1", len(slow))
	}
	got := drain(t, fast, 10)
	if len(got) != 10 {
		t.Fatalf("fast subscription received %d values, want 10", len(got))
	}
}

func TestOrdering(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[tick](64))
	defer cancel()

	for i := range 64 {
		b.Publish(tick{i})
	}

	for i, v := range drain(t, ch, 64) {
		if v.n != i {
			t.Fatalf("value %d = %d, want %d", i, v.n, i)
		}
	}
}

func TestSaturationDropsNewest(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[tick](2))
	defer cancel()

	for i := range 5 {
		b.Publish(tick{i})
	}

	got := drain(t, ch, 2)
	if len(got) != 2 || got[0].n != 0 || got[1].n != 1 {
		t.Fatalf("got %v, want the first two published values", got)
	}
	if len(ch) != 0 {
		t.Errorf("channel holds %d extra values, want 0", len(ch))
	}
}

func TestUnbufferedDropsWithoutReceiver(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[tick](0))
	defer cancel()

	b.Publish(tick{1}) // no receiver parked: dropped

	ready := make(chan struct{})
	got := make(chan tick, 1)
	go func() {
		close(ready)
		got <- <-ch
	}()
	<-ready

	// Publish until the receiver is parked and accepts one.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case v := <-got:
			if v.n != 2 {
				t.Fatalf("n = %d, want 2", v.n)
			}
			return
		case <-deadline:
			t.Fatal("unbuffered subscription never accepted a value")
		default:
			b.Publish(tick{2})
			time.Sleep(time.Millisecond)
		}
	}
}

func TestNegativeBufferIsUnbuffered(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[tick](-5))
	defer cancel()

	if got := cap(ch); got != 0 {
		t.Fatalf("cap = %d, want 0", got)
	}
}

func TestInterfaceSubscriptionReceivesImplementations(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[error](4))
	defer cancel()

	b.Publish(&failure{code: 12}) // concrete pointer implementing error
	b.Publish(tick{1})            // does not implement error

	got := drain(t, ch, 1)
	if len(got) != 1 || got[0].Error() != "failure 12" {
		t.Fatalf("got %v, want one failure", got)
	}
	if len(ch) != 0 {
		t.Errorf("channel holds %d extra values, want 0", len(ch))
	}
}

func TestInterfaceValuePublishesOnItsInterfaceType(t *testing.T) {
	var b topic.Broker
	errs, cancelErrs := b.Subscribe(topic.Buffer[error](4))
	defer cancelErrs()
	ptrs, cancelPtrs := b.Subscribe(topic.Buffer[*failure](4))
	defer cancelPtrs()

	var err error = &failure{code: 5}
	b.Publish(err) // topic is error, not *failure

	if got := drain(t, errs, 1); len(got) != 1 {
		t.Fatalf("error subscription got %v, want one value", got)
	}
	if len(ptrs) != 0 {
		t.Errorf("*failure subscription received %d values, want 0", len(ptrs))
	}
}

func TestAnySubscriptionReceivesEverything(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[any](8))
	defer cancel()

	b.Publish(tick{1})
	b.Publish("text")
	b.Publish(&failure{code: 1})
	var nilErr error
	b.Publish(nilErr) // nil interface value

	got := drain(t, ch, 4)
	if len(got) != 4 {
		t.Fatalf("got %d values, want 4", len(got))
	}
	if got[3] != nil {
		t.Errorf("fourth value = %v, want nil", got[3])
	}
}

func TestNilInterfaceReachesIdenticalTopic(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[error](2))
	defer cancel()

	var nilErr error
	b.Publish(nilErr)

	got := drain(t, ch, 1)
	if len(got) != 1 || got[0] != nil {
		t.Fatalf("got %v, want one nil error", got)
	}
}

func TestTransformation(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[tock](4),
		topic.From(func(t tick) (tock, bool) { return tock{n: t.n * 10}, true }),
	)
	defer cancel()

	b.Publish(tick{3})
	b.Publish(tock{7}) // direct match still available

	got := drain(t, ch, 2)
	if len(got) != 2 || got[0].n != 30 || got[1].n != 7 {
		t.Fatalf("got %v, want [{30} {7}]", got)
	}
}

func TestFilter(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[tick](8),
		topic.From(func(t tick) (tick, bool) { return t, t.n%2 == 0 }),
	)
	defer cancel()

	for i := range 6 {
		b.Publish(tick{i})
	}

	got := drain(t, ch, 3)
	if len(got) != 3 || got[0].n != 0 || got[1].n != 2 || got[2].n != 4 {
		t.Fatalf("got %v, want the even values", got)
	}
	if len(ch) != 0 {
		t.Errorf("channel holds %d extra values, want 0", len(ch))
	}
}

func TestMultipleSourcesFirstMatchWins(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[named](8),
		topic.From(func(t tick) (named, bool) { return named(fmt.Sprintf("tick:%d", t.n)), true }),
		topic.From(func(t tock) (named, bool) { return named(fmt.Sprintf("tock:%d", t.n)), true }),
		topic.From(func(t tick) (named, bool) { return "unreachable", true }),
	)
	defer cancel()

	b.Publish(tick{1})
	b.Publish(tock{2})
	b.Publish(named("direct"))

	got := drain(t, ch, 3)
	want := []named{"tick:1", "tock:2", "direct"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSourceForOwnTypeReplacesDirectMatch(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[tick](8),
		topic.From(func(t tick) (tick, bool) { return tick{n: -t.n}, true }),
	)
	defer cancel()

	b.Publish(tick{4})

	got := drain(t, ch, 1)
	if len(got) != 1 || got[0].n != -4 {
		t.Fatalf("got %v, want [{-4}]: the explicit source must win over direct matching", got)
	}
}

func TestInterfaceSourceAndConcreteSourceOrdering(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[named](8),
		topic.From(func(e error) (named, bool) { return named("iface:" + e.Error()), true }),
		topic.From(func(f *failure) (named, bool) { return named("concrete"), true }),
	)
	defer cancel()

	b.Publish(&failure{code: 9})

	got := drain(t, ch, 1)
	if len(got) != 1 || got[0] != "iface:failure 9" {
		t.Fatalf("got %v, want the first declared source to win", got)
	}
}

func TestTransformedValueIsDroppedWithoutFallthrough(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[tick](4),
		topic.From(func(t tick) (tick, bool) { return t, false }),
	)
	defer cancel()

	b.Publish(tick{1})

	if len(ch) != 0 {
		t.Fatalf("channel holds %d values, want 0", len(ch))
	}
}

func TestCancelClosesChannelAndUnregisters(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[tick](4))

	b.Publish(tick{1})
	cancel()
	b.Publish(tick{2}) // after cancel: never delivered

	var got []tick
	for v := range ch { // ends because the channel is closed
		got = append(got, v)
	}
	if len(got) != 1 || got[0].n != 1 {
		t.Fatalf("got %v, want the single value published before cancel", got)
	}
}

func TestCancelIsIdempotent(t *testing.T) {
	var b topic.Broker
	_, cancel := b.Subscribe[tick]()
	cancel()
	cancel()
	cancel()
}

func TestEquivalentSubscriptionsHaveIndependentLifecycles(t *testing.T) {
	var b topic.Broker
	first, cancelFirst := b.Subscribe(topic.Buffer[tick](4))
	second, cancelSecond := b.Subscribe(topic.Buffer[tick](4))
	defer cancelSecond()

	cancelFirst()
	b.Publish(tick{1})

	if _, ok := <-first; ok {
		t.Error("cancelled subscription received a value")
	}
	if got := (<-second).n; got != 1 {
		t.Errorf("n = %d, want 1", got)
	}
}

func TestCancelDuringPublicationIsSafe(t *testing.T) {
	var b topic.Broker
	var wg sync.WaitGroup

	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					b.Publish(tick{1})
					b.Publish(&failure{code: 1})
				}
			}
		}()
	}

	for range 200 {
		ch, cancel := b.Subscribe(topic.Buffer[tick](2))
		errs, cancelErrs := b.Subscribe(topic.Buffer[error](2))
		anys, cancelAnys := b.Subscribe(topic.Buffer[any](2))
		go func() {
			for range ch {
			}
		}()
		cancel()
		cancelErrs()
		cancelAnys()
		<-errs
		<-anys
	}

	close(stop)
	wg.Wait()
}

func TestConcurrentPublishAndSubscribe(t *testing.T) {
	var b topic.Broker
	var publishers, consumers sync.WaitGroup

	received := make([]atomic.Int64, 8)
	cancels := make([]func(), len(received))
	stop := make(chan struct{})

	for i := range received {
		ch, cancel := b.Subscribe(topic.Buffer[tick](32))
		cancels[i] = cancel
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for range ch {
				received[i].Add(1)
			}
		}()
	}

	for range 8 {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					b.Publish(tick{1})
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	publishers.Wait()

	for _, cancel := range cancels {
		cancel()
	}
	consumers.Wait() // every channel is closed, so every range has ended

	var total int64
	for i := range received {
		total += received[i].Load()
	}
	if total == 0 {
		t.Error("no values were delivered")
	}
}

func TestManyDistinctTopicTypesRouteIndependently(t *testing.T) {
	var b topic.Broker
	cancels := make([]func(), 0, 40)
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	register := func(cancel func()) { cancels = append(cancels, cancel) }

	register(subscribeAndIgnore[int8](&b))
	register(subscribeAndIgnore[int16](&b))
	register(subscribeAndIgnore[int32](&b))
	register(subscribeAndIgnore[int64](&b))
	register(subscribeAndIgnore[uint8](&b))
	register(subscribeAndIgnore[uint16](&b))
	register(subscribeAndIgnore[uint32](&b))
	register(subscribeAndIgnore[uint64](&b))
	register(subscribeAndIgnore[float32](&b))
	register(subscribeAndIgnore[float64](&b))
	register(subscribeAndIgnore[complex64](&b))
	register(subscribeAndIgnore[complex128](&b))
	register(subscribeAndIgnore[string](&b))
	register(subscribeAndIgnore[bool](&b))
	register(subscribeAndIgnore[uintptr](&b))
	register(subscribeAndIgnore[tock](&b))
	register(subscribeAndIgnore[named](&b))

	ch, cancel := b.Subscribe(topic.Buffer[tick](2))
	register(cancel)
	ints, cancelInts := b.Subscribe(topic.Buffer[int16](2))
	register(cancelInts)

	b.Publish(tick{99})
	b.Publish(int16(3))

	if got := (<-ch).n; got != 99 {
		t.Errorf("tick n = %d, want 99", got)
	}
	if got := <-ints; got != 3 {
		t.Errorf("int16 = %d, want 3", got)
	}
}

func subscribeAndIgnore[T any](b *topic.Broker) func() {
	_, cancel := b.Subscribe(topic.Buffer[T](1))
	return cancel
}

func TestDynamicSubscriptionStillMatchesConcreteSources(t *testing.T) {
	// A subscription with an interface source leaves the typed routing path,
	// but its concrete sources must keep behaving identically.
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[named](8),
		topic.From(func(e error) (named, bool) { return named("err"), true }),
		topic.From(func(t tick) (named, bool) { return named(fmt.Sprintf("tick:%d", t.n)), true }),
	)
	defer cancel()

	b.Publish(tick{5})
	b.Publish(&failure{code: 1})
	b.Publish(named("direct"))
	b.Publish(tock{1}) // matches nothing

	got := drain(t, ch, 3)
	want := []named{"tick:5", "err", "direct"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("got %v, want %v", got, want)
	}
	if len(ch) != 0 {
		t.Errorf("channel holds %d extra values, want 0", len(ch))
	}
}

func TestConcreteSourceIgnoresValuesPublishedThroughAnInterface(t *testing.T) {
	// A concrete source matches the published static type, so a *failure
	// inside an error variable, published on the topic error, does not reach
	// a source that names *failure.
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[named](8),
		topic.From(func(f *failure) (named, bool) { return named("concrete"), true }),
	)
	defer cancel()

	var err error = &failure{code: 1}
	b.Publish(err)               // topic is error: the source does not match
	b.Publish(&failure{code: 2}) // topic is *failure: the source matches

	got := drain(t, ch, 1)
	if len(got) != 1 || got[0] != "concrete" {
		t.Fatalf("got %v, want [concrete]", got)
	}
	if len(ch) != 0 {
		t.Errorf("channel holds %d extra values, want 0", len(ch))
	}
}

// TestInterfaceSourceMatchesDynamicType pins the documented divergence between
// the two kinds of source: an interface source is matched against the boxed
// value, whose dynamic type may be more specific than the published type. See
// DESIGN.md section 7.
func TestInterfaceSourceMatchesDynamicType(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(
		topic.Buffer[named](4),
		topic.From(func(s fmt.Stringer) (named, bool) { return named("stringer:" + s.String()), true }),
	)
	defer cancel()

	// The published type is error, which is not assignable to fmt.Stringer,
	// but the value behind it implements fmt.Stringer and so matches.
	var err error = &described{code: 3}
	b.Publish(err)

	got := drain(t, ch, 1)
	if len(got) != 1 || got[0] != "stringer:described 3" {
		t.Fatalf("got %v, want [stringer:described 3]", got)
	}
}

func TestZeroOptionHasNoEffect(t *testing.T) {
	var b topic.Broker
	var zero topic.Option[tick]
	ch, cancel := b.Subscribe(zero)
	defer cancel()

	if got := cap(ch); got != 1 {
		t.Errorf("cap = %d, want the default 1", got)
	}
	b.Publish(tick{8})
	if got := (<-ch).n; got != 8 {
		t.Errorf("n = %d, want 8", got)
	}
}

func TestLastBufferWins(t *testing.T) {
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[tick](8), topic.Buffer[tick](3))
	defer cancel()

	if got := cap(ch); got != 3 {
		t.Fatalf("cap = %d, want 3", got)
	}
}

func TestOrderingForInterfaceSubscription(t *testing.T) {
	// The dynamic routing path must preserve order just as the typed one does.
	var b topic.Broker
	ch, cancel := b.Subscribe(topic.Buffer[any](64))
	defer cancel()

	for i := range 64 {
		b.Publish(tick{i})
	}

	for i, v := range drain(t, ch, 64) {
		got, ok := v.(tick)
		if !ok || got.n != i {
			t.Fatalf("value %d = %v, want tick{%d}", i, v, i)
		}
	}
}

// described implements both error and fmt.Stringer.
type described struct{ code int }

func (d *described) Error() string  { return fmt.Sprintf("described %d", d.code) }
func (d *described) String() string { return fmt.Sprintf("described %d", d.code) }

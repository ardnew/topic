package topic_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ardnew/topic"
)

const testTimeout = 3 * time.Second

func TestSubscribeDirect(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[string]().Topics(t.Context())
	b.Publish("hello")

	select {
	case got := <-topics:
		if got != "hello" {
			t.Fatalf("topic = %q, want %q", got, "hello")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for topic")
	}
}

func TestSubscribeDefaultBufferLen(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[int]().Topics(t.Context())
	if got := cap(topics); got != topic.DefaultBufferLen {
		t.Fatalf("channel capacity = %d, want %d", got, topic.DefaultBufferLen)
	}
}

func TestSubscribeAppliesOptions(t *testing.T) {
	var b topic.Broker
	var calls int
	_ = b.Subscribe(func(*topic.Receiver[int]) { calls++ })
	if calls != 1 {
		t.Fatalf("option calls = %d, want 1", calls)
	}
}

func TestWithBufferLen(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe(
		topic.WithBufferLen[int](3),
	).Topics(t.Context())
	if got := cap(topics); got != 3 {
		t.Fatalf("channel capacity = %d, want 3", got)
	}
}

func TestWithBufferLenLastOptionWins(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe(
		topic.WithBufferLen[int](1),
		topic.WithBufferLen[int](2),
	).Topics(t.Context())
	if got := cap(topics); got != 2 {
		t.Fatalf("channel capacity = %d, want 2", got)
	}
}

func TestWithBufferLenAppliesToReceiver(t *testing.T) {
	var b topic.Broker
	receiver := b.Subscribe(
		topic.WithBufferLen[int](1),
	)
	firstTopics := receiver.Topics(t.Context())
	secondTopics := receiver.Topics(t.Context())
	defaultTopics := b.Subscribe[int]().Topics(t.Context())

	if got := cap(firstTopics); got != 1 {
		t.Fatalf("first channel capacity = %d, want 1", got)
	}
	if got := cap(secondTopics); got != 1 {
		t.Fatalf("second channel capacity = %d, want 1", got)
	}
	if got := cap(defaultTopics); got != topic.DefaultBufferLen {
		t.Fatalf(
			"default channel capacity = %d, want %d",
			got,
			topic.DefaultBufferLen,
		)
	}
}

func TestWithBufferLenZero(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe(
		topic.WithBufferLen[int](0),
	).Topics(t.Context())
	if got := cap(topics); got != 0 {
		t.Fatalf("channel capacity = %d, want 0", got)
	}

	b.Publish(1)
	select {
	case got := <-topics:
		t.Fatalf("unexpected topic from unbuffered channel: %d", got)
	default:
		// Correct: non-blocking delivery requires a waiting receiver.
	}

	received := make(chan int, 1)
	go func() { received <- <-topics }()
	deadline := time.After(testTimeout)
	for {
		b.Publish(2)
		select {
		case got := <-received:
			if got != 2 {
				t.Fatalf("topic = %d, want 2", got)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for unbuffered delivery")
		default:
			runtime.Gosched()
		}
	}
}

func TestWithBufferLenDropsWhenFull(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[int](
		topic.WithBufferLen[int](1),
	).Topics(t.Context())
	b.Publish(1)
	b.Publish(2)

	if got := <-topics; got != 1 {
		t.Fatalf("topic = %d, want 1", got)
	}
	select {
	case got := <-topics:
		t.Fatalf("unexpected topic after buffer filled: %d", got)
	default:
		// Correct: the second topic was dropped.
	}
}

func TestWithBufferLenPanicsForNegativeLength(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithBufferLen did not panic")
		}
	}()
	_ = topic.WithBufferLen[int](-1)
}

func TestSubscribeCancel(t *testing.T) {
	var b topic.Broker
	ctx, cancel := context.WithCancel(t.Context())
	topics := b.Subscribe[int]().Topics(ctx)
	cancel()

	select {
	case _, ok := <-topics:
		if ok {
			t.Fatal("channel should be closed")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel close")
	}
}

type source struct{ text string }

type target struct{ summary string }

func TestSubscribeFrom(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[target]().
		From(func(topic source) (target, bool) {
			return target{summary: topic.text}, true
		}).
		Topics(t.Context())
	b.Publish(source{text: "hello"})

	select {
	case got := <-topics:
		if got.summary != "hello" {
			t.Fatalf("topic = %q, want %q", got.summary, "hello")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for converted topic")
	}
}

func TestSubscribeFromReject(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[target]().
		From(func(source) (target, bool) {
			return target{}, false
		}).
		Topics(t.Context())
	b.Publish(source{text: "ignored"})

	select {
	case got := <-topics:
		t.Fatalf("unexpected delivery: %v", got)
	case <-time.After(50 * time.Millisecond):
		// Correct: nothing received.
	}
}

func TestSubscribeUnrelated(t *testing.T) {
	var b topic.Broker
	ctx := t.Context()

	topics := b.Subscribe[target]().Topics(ctx)
	b.Publish(source{text: "unrelated"})

	select {
	case got := <-topics:
		t.Fatalf("unexpected topic: %v", got)
	case <-time.After(50 * time.Millisecond):
		// Correct: nothing received.
	}
}

func TestSubscribeFanout(t *testing.T) {
	var b topic.Broker
	ctx := t.Context()

	firstTopics := b.Subscribe[string]().Topics(ctx)
	secondTopics := b.Subscribe[string]().Topics(ctx)
	b.Publish("broadcast")

	for i, topics := range []<-chan string{firstTopics, secondTopics} {
		select {
		case got := <-topics:
			if got != "broadcast" {
				t.Fatalf("subscriber %d got %q, want %q", i, got, "broadcast")
			}
		case <-time.After(testTimeout):
			t.Fatalf("subscriber %d timed out", i)
		}
	}
}

type namer interface{ Name() string }

type alpha struct{ name string }
type beta struct{ name string }

func (a alpha) Name() string { return a.name }
func (b beta) Name() string  { return b.name }

func TestSubscribeInterface(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[namer]().Topics(t.Context())
	b.Publish(alpha{name: "alpha"})
	b.Publish(beta{name: "beta"})

	got := make([]string, 0, 2)
	deadline := time.After(testTimeout)
	for len(got) < 2 {
		select {
		case topic := <-topics:
			got = append(got, topic.Name())
		case <-deadline:
			t.Fatalf("timed out after receiving %v", got)
		}
	}
	if got[0] != "alpha" || got[1] != "beta" {
		t.Fatalf("got %v, want [alpha beta]", got)
	}
}

func TestSubscribeFromMultiple(t *testing.T) {
	var b topic.Broker
	type sourceA struct{ value int }
	type sourceB struct{ value int }
	type result struct{ total int }

	topics := b.
		Subscribe[result]().
		From(func(topic sourceA) (result, bool) {
			return result{total: topic.value}, true
		}).
		From(func(topic sourceB) (result, bool) {
			return result{total: topic.value * 10}, true
		}).
		Topics(t.Context())

	b.Publish(sourceA{value: 1})
	b.Publish(sourceB{value: 2})

	got := make([]int, 0, 2)
	deadline := time.After(testTimeout)
	for len(got) < 2 {
		select {
		case topic := <-topics:
			got = append(got, topic.total)
		case <-deadline:
			t.Fatalf("timed out after receiving %v", got)
		}
	}
	if got[0] != 1 || got[1] != 20 {
		t.Fatalf("got %v, want [1 20]", got)
	}
}

func TestSubscribeFromFirstInterfaceOwnsDecision(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[target]().
		From(func(topic namer) (target, bool) {
			return target{summary: "interface:" + topic.Name()}, true
		}).
		From(func(topic alpha) (target, bool) {
			return target{summary: "concrete:" + topic.Name()}, true
		}).
		Topics(t.Context())

	b.Publish(alpha{name: "alpha"})

	select {
	case got := <-topics:
		if got.summary != "interface:alpha" {
			t.Fatalf("topic = %q, want %q", got.summary, "interface:alpha")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for topic")
	}
}

func TestSubscribeDirectAfterUnmatchedAdapter(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[target]().
		From(func(topic source) (target, bool) {
			return target{summary: topic.text}, true
		}).
		Topics(t.Context())

	b.Publish(target{summary: "direct"})

	select {
	case got := <-topics:
		if got.summary != "direct" {
			t.Fatalf("topic = %q, want %q", got.summary, "direct")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for topic")
	}
}

func TestPublishExactAllocations(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[smallAllocationTopic]().Topics(t.Context())
	value := smallAllocationTopic{sequence: 1, enabled: true}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
		<-topics
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestPublishWithoutSubscribersAllocations(t *testing.T) {
	var b topic.Broker
	value := smallAllocationTopic{sequence: 1, enabled: true}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestPublishUnmatchedAllocations(t *testing.T) {
	var b topic.Broker
	_ = b.Subscribe[string]().Topics(t.Context())
	value := smallAllocationTopic{sequence: 1, enabled: true}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
	})
	if allocations != 1 {
		t.Fatalf("allocations = %v, want 1", allocations)
	}
}

func TestPublishPreboxedInterfaceAllocations(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[namer]().Topics(t.Context())
	value := namer(&alpha{name: "alpha"})

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
		<-topics
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestPublishPointerToInterfaceAllocations(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[namer]().Topics(t.Context())
	value := &alpha{name: "alpha"}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
		<-topics
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestPublishValueToInterfaceFanoutAllocations(t *testing.T) {
	var b topic.Broker
	first := b.Subscribe[namer]().Topics(t.Context())
	second := b.Subscribe[namer]().Topics(t.Context())
	value := alpha{name: "alpha"}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
		<-first
		<-second
	})
	if allocations != 1 {
		t.Fatalf("allocations = %v, want 1", allocations)
	}
}

func TestPublishFilteredAllocations(t *testing.T) {
	var b topic.Broker
	_ = b.
		Subscribe[smallAllocationTopic]().
		From(func(value smallAllocationTopic) (smallAllocationTopic, bool) {
			return value, false
		}).
		Topics(t.Context())
	value := smallAllocationTopic{sequence: 1, enabled: true}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestPublishConvertedAllocations(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[target]().
		From(func(value smallAllocationTopic) (target, bool) {
			return target{summary: "converted"}, value.enabled
		}).
		Topics(t.Context())
	value := smallAllocationTopic{sequence: 1, enabled: true}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
		<-topics
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestPublishFanoutAllocations(t *testing.T) {
	var b topic.Broker
	first := b.Subscribe[smallAllocationTopic]().Topics(t.Context())
	second := b.Subscribe[smallAllocationTopic]().Topics(t.Context())
	value := smallAllocationTopic{sequence: 1, enabled: true}

	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(value)
		<-first
		<-second
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

type smallAllocationTopic struct {
	sequence uint64
	enabled  bool
}

func TestBrokerIndependent(t *testing.T) {
	var first topic.Broker
	var second topic.Broker
	firstTopics := first.Subscribe[string]().Topics(t.Context())
	secondTopics := second.Subscribe[string]().Topics(t.Context())

	first.Publish("first only")
	var got string
	select {
	case got = <-firstTopics:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for first broker")
	}

	if got != "first only" {
		t.Fatalf("topic = %q, want %q", got, "first only")
	}

	select {
	case got := <-secondTopics:
		t.Fatalf("independent broker received unexpected topic: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBrokerZeroValue(t *testing.T) {
	var b topic.Broker
	b.Publish("discarded without subscribers")

	topics := b.Subscribe[string]().Topics(t.Context())
	b.Publish("delivered")

	select {
	case topic := <-topics:
		if topic != "delivered" {
			t.Fatalf("topic = %q, want %q", topic, "delivered")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for topic")
	}
}

func TestFromFilter(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[int]().
		From(func(topic int) (int, bool) {
			return topic, topic > 0 && topic%2 == 0
		}).
		Topics(t.Context())

	b.Publish(-2)
	b.Publish(1)
	b.Publish(2)

	select {
	case topic := <-topics:
		if topic != 2 {
			t.Fatalf("topic = %d, want 2", topic)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for matching topic")
	}
}

func TestFromConvertsAndFilters(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[target]().
		From(func(topic source) (target, bool) {
			converted := target{summary: topic.text}
			return converted, converted.summary == "delivered"
		}).
		Topics(t.Context())

	b.Publish(source{text: "filtered"})
	b.Publish(source{text: "delivered"})

	select {
	case topic := <-topics:
		if topic.summary != "delivered" {
			t.Fatalf("topic = %q, want %q", topic.summary, "delivered")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for converted topic")
	}
}

func TestSubscribeCancelConcurrentPublish(t *testing.T) {
	for range 100 {
		var b topic.Broker
		ctx, cancel := context.WithCancel(t.Context())
		topics := b.Subscribe[int]().Topics(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range 100 {
				b.Publish(1)
			}
		}()
		cancel()
		for range topics {
		}
		<-done
	}
}

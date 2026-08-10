package topic_test

import (
	"errors"
	"fmt"

	"github.com/ardnew/topic"
)

// A subscriber selects a Go type; publishers send ordinary values of it.
func Example() {
	type Tick struct{ N int }

	var b topic.Broker

	ch, cancel := b.Subscribe[Tick]()
	defer cancel()

	b.Publish(Tick{N: 1})

	fmt.Println((<-ch).N)
	// Output: 1
}

// Distinct types are distinct topics, with no identifiers to declare.
func ExampleBroker_Publish() {
	type Login struct{ User string }
	type Logout struct{ User string }

	var b topic.Broker

	logins, cancelLogins := b.Subscribe[Login]()
	defer cancelLogins()
	logouts, cancelLogouts := b.Subscribe[Logout]()
	defer cancelLogouts()

	b.Publish(Logout{User: "ada"})
	b.Publish(Login{User: "grace"})

	fmt.Println((<-logins).User, "in")
	fmt.Println((<-logouts).User, "out")
	// Output:
	// grace in
	// ada out
}

// A subscription to an interface type receives every published value that
// implements it.
func ExampleBroker_Subscribe_interface() {
	var b topic.Broker

	errs, cancel := b.Subscribe(topic.Buffer[error](4))
	defer cancel()

	b.Publish(fmt.Errorf("disk full")) // implements error
	b.Publish(42)                      // does not

	fmt.Println(<-errs)
	fmt.Println("pending:", len(errs))
	// Output:
	// disk full
	// pending: 0
}

// A subscription to any receives every published value.
func ExampleBroker_Subscribe_any() {
	var b topic.Broker

	all, cancel := b.Subscribe(topic.Buffer[any](4))
	defer cancel()

	b.Publish("hello")
	b.Publish(7)
	b.Publish(errors.New("oops"))

	for len(all) > 0 {
		v := <-all
		fmt.Printf("%T %v\n", v, v)
	}
	// Output:
	// string hello
	// int 7
	// *errors.errorString oops
}

// Cancelling closes the channel, so a range loop over it ends.
func ExampleBroker_Subscribe_cancel() {
	type Tick struct{ N int }

	var b topic.Broker

	ch, cancel := b.Subscribe(topic.Buffer[Tick](4))

	for i := range 3 {
		b.Publish(Tick{N: i})
	}
	cancel()
	b.Publish(Tick{N: 99}) // never delivered: the subscription is gone

	for tick := range ch { // values accepted before cancel remain receivable
		fmt.Println(tick.N)
	}
	fmt.Println("done")
	// Output:
	// 0
	// 1
	// 2
	// done
}

// From converts an explicitly chosen source type into the subscription's type.
func ExampleFrom() {
	type Celsius float64
	type Fahrenheit float64

	var b topic.Broker

	ch, cancel := b.Subscribe(
		topic.Buffer[Fahrenheit](4),
		topic.From(func(c Celsius) (Fahrenheit, bool) { return Fahrenheit(c*9/5 + 32), true }),
	)
	defer cancel()

	b.Publish(Celsius(100))    // converted
	b.Publish(Fahrenheit(-40)) // direct match still works
	b.Publish("not a reading") // unrelated type

	fmt.Println(<-ch)
	fmt.Println(<-ch)
	// Output:
	// 212
	// -40
}

// Reporting false from the same function drops the value, which is filtering.
func ExampleFrom_filter() {
	type Reading struct{ Volts float64 }

	var b topic.Broker

	ch, cancel := b.Subscribe(
		topic.Buffer[Reading](8),
		topic.From(func(r Reading) (Reading, bool) { return r, r.Volts > 3.0 }),
	)
	defer cancel()

	for _, v := range []float64{1.5, 3.3, 2.0, 5.0} {
		b.Publish(Reading{Volts: v})
	}

	for len(ch) > 0 {
		fmt.Println((<-ch).Volts)
	}
	// Output:
	// 3.3
	// 5
}

// Several source types can feed one subscription. The first source that
// matches a publication decides its fate, so ordering is deterministic.
func ExampleFrom_multiple() {
	type Bytes int64
	type Packet struct{ Size int }
	type Frame struct{ Size int }

	var b topic.Broker

	ch, cancel := b.Subscribe(
		topic.Buffer[Bytes](8),
		topic.From(func(p Packet) (Bytes, bool) { return Bytes(p.Size), true }),
		topic.From(func(f Frame) (Bytes, bool) { return Bytes(f.Size), f.Size > 0 }),
	)
	defer cancel()

	b.Publish(Packet{Size: 1500})
	b.Publish(Frame{Size: 0}) // rejected by its source
	b.Publish(Frame{Size: 64})
	b.Publish(Bytes(9))

	for len(ch) > 0 {
		fmt.Println(<-ch)
	}
	// Output:
	// 1500
	// 64
	// 9
}

// Buffer sets how much a subscription can hold. A publication that a
// subscription cannot accept immediately is dropped for that subscription
// alone.
func ExampleBuffer() {
	type Tick struct{ N int }

	var b topic.Broker

	small, cancelSmall := b.Subscribe(topic.Buffer[Tick](2))
	defer cancelSmall()
	large, cancelLarge := b.Subscribe(topic.Buffer[Tick](8))
	defer cancelLarge()

	for i := range 5 {
		b.Publish(Tick{N: i})
	}

	fmt.Println("small held:", len(small))
	fmt.Println("large held:", len(large))
	// Output:
	// small held: 2
	// large held: 5
}

package log

import (
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ardnew/topic"
)

const testTimeout = 3 * time.Second

func framePackage(frame runtime.Frame) string {
	name := frame.Function
	if name == "" {
		fn := runtime.FuncForPC(frame.PC)
		if fn == nil {
			return ""
		}
		name = fn.Name()
	}
	start := strings.LastIndexByte(name, '/') + 1
	if dot := strings.IndexByte(name[start:], '.'); dot >= 0 {
		return name[:start+dot]
	}
	return name
}

func TestWrapCallSite(t *testing.T) {
	_, file, line, _ := runtime.Caller(0)
	record := Wrap(slog.LevelInfo, "ready")

	if record.Topic != "ready" || record.Level != slog.LevelInfo {
		t.Fatalf("record = %+v", record)
	}
	if record.Time.IsZero() {
		t.Fatal("timestamp is zero")
	}
	pkg := framePackage(record.Frame)
	if pkg != "github.com/ardnew/topic/log" {
		t.Fatalf("source package = %q", pkg)
	}
	if record.Frame.File != file || record.Frame.Line != line+1 {
		t.Fatalf("source = %s:%d, want %s:%d",
			record.Frame.File, record.Frame.Line, file, line+1)
	}
}

func TestBrokerPublishCallSite(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[Record[string]]().Topics(t.Context())
	_, file, line, _ := runtime.Caller(0)
	b.Publish(Wrap(slog.LevelInfo, "ready"))

	got := receive(t, topics)
	if pkg := framePackage(got.Frame); pkg != "github.com/ardnew/topic/log" {
		t.Fatalf("source package = %q", pkg)
	}
	if got.Frame.File != file || got.Frame.Line != line+1 {
		t.Fatalf("source = %s:%d, want %s:%d",
			filepath.Base(got.Frame.File), got.Frame.Line, filepath.Base(file), line+1)
	}
}

func TestBrokerPublishRecord(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe[Record[string]]().Topics(t.Context())
	want := Record[string]{
		Topic: "original",
		Level: slog.LevelError,
		Time:  time.Unix(10, 20),
		Frame: runtime.Frame{
			Function: "example.com/origin.Forward",
			File:     "origin.go",
			Line:     7,
		},
	}

	b.Publish(want)

	if got := receive(t, topics); got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

func TestUnwrap(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[string]().
		From(Unwrap[string]).
		Topics(t.Context())

	b.Publish(Wrap(slog.LevelInfo, "ready"))
	if got := receive(t, topics); got != "ready" {
		t.Fatalf("topic = %q, want %q", got, "ready")
	}
}

func TestAtLeastDynamic(t *testing.T) {
	var min slog.LevelVar
	allow := AtLeast[string](&min)
	record := Record[string]{Level: slog.LevelInfo}

	min.Set(slog.LevelDebug)
	if _, accepted := allow(record); !accepted {
		t.Fatal("Info rejected at Debug")
	}
	min.Set(slog.LevelWarn)
	if _, accepted := allow(record); accepted {
		t.Fatal("Info accepted at Warn")
	}
}

func TestAtLeast(t *testing.T) {
	var b topic.Broker
	topics := b.
		Subscribe[Record[string]]().
		From(AtLeast[string](slog.LevelInfo)).
		Topics(t.Context())

	b.Publish(Wrap(slog.LevelDebug, "filtered"))
	b.Publish(Wrap(slog.LevelInfo, "delivered"))

	if got := receive(t, topics); got.Topic != "delivered" {
		t.Fatalf("topic = %q, want %q", got.Topic, "delivered")
	}
}

func receive[T any](t *testing.T, topics <-chan T) T {
	t.Helper()
	select {
	case got := <-topics:
		return got
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for record")
		var zero T
		return zero
	}
}

package log

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestNewCallSite(t *testing.T) {
	attrs := []slog.Attr{slog.String("state", "ready")}
	_, file, line, _ := runtime.Caller(0)
	record := New(slog.LevelInfo, "started", "topic", attrs...)
	attrs[0] = slog.String("state", "changed")

	if record.Topic != "topic" || record.Level != slog.LevelInfo ||
		record.Message != "started" {
		t.Fatalf("record = %+v", record)
	}
	if len(record.Attrs) != 1 || record.Attrs[0].Value.String() != "ready" {
		t.Fatalf("attrs = %v, want copied state=ready", record.Attrs)
	}
	if record.Time.IsZero() {
		t.Fatal("timestamp is zero")
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

	got := receive(t, topics)
	if got.Topic != want.Topic || got.Level != want.Level ||
		!got.Time.Equal(want.Time) || got.Frame != want.Frame ||
		got.Message != want.Message || len(got.Attrs) != 0 {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

func TestWithWrapPlainTopicCapturesPublishCallSite(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe(WithWrap[string](slog.LevelInfo, 0)).Topics(t.Context())
	_, file, line, _ := runtime.Caller(0)
	b.Publish("ready")

	got := receive(t, topics)
	if got.Topic != "ready" || got.Level != slog.LevelInfo {
		t.Fatalf("record = %+v", got)
	}
	if got.Time.IsZero() {
		t.Fatal("timestamp is zero")
	}
	if got.Frame.File != file || got.Frame.Line != line+1 {
		t.Fatalf("source = %s:%d, want %s:%d",
			filepath.Base(got.Frame.File), got.Frame.Line, filepath.Base(file), line+1)
	}

	file, line = publishString(&b, "again")
	got = receive(t, topics)
	if got.Frame.File != file || got.Frame.Line != line {
		t.Fatalf("cached source = %s:%d, want %s:%d",
			filepath.Base(got.Frame.File), got.Frame.Line, filepath.Base(file), line)
	}
}

//go:noinline
func publishString(b *topic.Broker, value string) (string, int) {
	_, file, line, _ := runtime.Caller(0)
	b.Publish(value)
	return file, line + 1
}

func TestWithWrapAllocations(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe(WithWrap[int](slog.LevelInfo, 0)).Topics(t.Context())
	allocations := testing.AllocsPerRun(1000, func() {
		b.Publish(42)
		<-topics
	})
	if allocations != 0 {
		t.Fatalf("allocations = %v, want 0", allocations)
	}
}

func TestWithWrapFiltersAndPreservesRecords(t *testing.T) {
	var b topic.Broker
	topics := b.Subscribe(WithWrap[string](slog.LevelInfo, 0)).Topics(t.Context())
	b.Publish(Record[string]{Topic: "filtered", Level: slog.LevelDebug})
	want := New(
		slog.LevelWarn,
		"delivered",
		"preserved",
		slog.String("state", "original"),
	)
	b.Publish(want)

	got := receive(t, topics)
	if got.Topic != want.Topic || got.Level != want.Level ||
		!got.Time.Equal(want.Time) || got.Frame != want.Frame ||
		got.Message != want.Message || len(got.Attrs) != 1 ||
		!got.Attrs[0].Equal(want.Attrs[0]) {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

func TestRecordSlog(t *testing.T) {
	record := New(
		slog.LevelWarn,
		"slow request",
		"request",
		slog.Group("http", slog.Int("status", 503)),
	)

	first := record.Slog()
	second := record.Slog()
	first.AddAttrs(slog.String("copy", "first"))

	if first.Time != record.Time || first.Level != record.Level ||
		first.Message != record.Message || first.PC != record.Frame.PC {
		t.Fatalf("slog record = %+v, source = %+v", first, record)
	}
	if first.NumAttrs() != 2 || second.NumAttrs() != 1 {
		t.Fatalf("attribute counts = %d and %d, want 2 and 1",
			first.NumAttrs(), second.NumAttrs())
	}
	source := second.Source()
	if source == nil || source.File != record.Frame.File ||
		source.Line != record.Frame.Line {
		t.Fatalf("source = %+v, want %s:%d",
			source, record.Frame.File, record.Frame.Line)
	}
}

type lazyLogValue struct {
	calls *int
}

func (value lazyLogValue) LogValue() slog.Value {
	*value.calls = *value.calls + 1
	return slog.StringValue("resolved")
}

func TestRecordHandleJSONGroupsAndLazyValues(t *testing.T) {
	var output bytes.Buffer
	var min slog.LevelVar
	min.Set(slog.LevelInfo)
	handler := slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: &min}).
		WithGroup("component").
		WithAttrs([]slog.Attr{slog.String("name", "lang")})
	calls := 0

	debug := New(
		slog.LevelDebug,
		"hidden",
		"debug",
		slog.Any("value", lazyLogValue{calls: &calls}),
	)
	if err := debug.Handle(t.Context(), handler); err != nil {
		t.Fatalf("Handle(Debug) error = %v", err)
	}
	if calls != 0 || output.Len() != 0 {
		t.Fatalf("disabled handler resolved %d values and wrote %q", calls, output.String())
	}

	info := New(
		slog.LevelInfo,
		"ready",
		"info",
		slog.Group(
			"request",
			slog.Int("id", 42),
			slog.Any("value", lazyLogValue{calls: &calls}),
		),
	)
	if err := info.Handle(t.Context(), handler); err != nil {
		t.Fatalf("Handle(Info) error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("LogValue calls = %d, want 1", calls)
	}

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if entry["msg"] != "ready" || entry["level"] != "INFO" {
		t.Fatalf("entry = %#v", entry)
	}
	component, ok := entry["component"].(map[string]any)
	if !ok || component["name"] != "lang" {
		t.Fatalf("component = %#v", entry["component"])
	}
	request, ok := component["request"].(map[string]any)
	if !ok || request["id"] != float64(42) || request["value"] != "resolved" {
		t.Fatalf("request = %#v", component["request"])
	}
}

type failingHandler struct {
	err        error
	enabledCtx context.Context
	handleCtx  context.Context
}

func (handler *failingHandler) Enabled(ctx context.Context, _ slog.Level) bool {
	handler.enabledCtx = ctx
	return true
}

func (handler *failingHandler) Handle(ctx context.Context, _ slog.Record) error {
	handler.handleCtx = ctx
	return handler.err
}

func (handler *failingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return handler
}

func (handler *failingHandler) WithGroup(string) slog.Handler {
	return handler
}

func TestRecordHandleUsesBackgroundAndReturnsError(t *testing.T) {
	want := errors.New("write failed")
	handler := &failingHandler{err: want}

	if err := New(slog.LevelError, "failed", "topic").Handle(nil, handler); !errors.Is(err, want) {
		t.Fatalf("Handle error = %v, want %v", err, want)
	}
	if handler.enabledCtx == nil || handler.handleCtx == nil {
		t.Fatal("nil context passed to handler")
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

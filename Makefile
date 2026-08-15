SHELL := $(firstword $(shell which bash sh))

moddir := $(shell go list -f '{{.Dir}}' .)
modimp := $(shell go list -f '{{.ImportPath}}' .)

output  := $(or $(OUTPUT),$(moddir)/dist)
profile := $(output)/cover.out
report := $(output)/cover.html

# Sources every generated artifact depends on. A profile is regenerated
# when the code it measures changes, and reused otherwise.
sources := $(wildcard $(moddir)/*.go)

# Profiling and debugging artifacts.
cpuprof   := $(output)/cpu.prof
memprof   := $(output)/mem.prof
mutexprof := $(output)/mutex.prof
blockprof := $(output)/block.prof
traceout  := $(output)/trace.out
callgraph := $(output)/cpu.svg
binary    := $(output)/topic.test
baseline  := $(output)/bench.base.txt
current   := $(output)/bench.new.txt

# Each of the following can be overridden from the command line, e.g.:
#   make test BENCH=BenchmarkFoo COUNT=5 FLAGS=-tags=foo
package := $(or $(PKG),./...)
run     := $(or $(RUN),.)
bench   := $(or $(BENCH),.)
count   := $(or $(COUNT),1)
time    := $(or $(TIME),1s)
extra   := $(FLAGS)

# Profiling and debugging name exactly one package, never a pattern, because
# a profile, a binary, and a debugger session each belong to one.
single := $(or $(PKG),.)

# Profiling and debugging accept the same overrides, plus:
#   make flame BENCH=BenchmarkPublishDirect TIME=5s
#   make list FUNC=Publish
#   make debug-test RUN=TestDirectDelivery HEADLESS=1
func  := $(or $(FUNC),.)
addr  := $(or $(ADDR),localhost:8080)
nodes := $(or $(NODES),20)

# Detail level for the escape analysis; -m=1 reports decisions, -m=2 also
# reports why.
mlevel := $(or $(M),2)

# Sampling rates for the allocation and contention profiles. Both defaults
# record every event, which is what makes a low-traffic profile legible and
# what makes a hot one slower; raise them to sample instead.
memrate   := $(or $(MEMRATE),1)
mutexrate := $(or $(MUTEXRATE),1)
blockrate := $(or $(BLOCKRATE),1)

# benchstat wants enough samples to reach significance; ten is its
# conventional floor for p < 0.05.
stats := $(or $(STATS),10)

# Delve listens instead of prompting when HEADLESS is non-empty, which is
# how an editor or a second terminal attaches to the session.
dlvport  := $(or $(DLVPORT),2345)
dlvflags := $(if $(HEADLESS),--headless --listen=:$(dlvport) --api-version=2 --accept-multiclient)

# Any non-empty V enables verbose output from the test binary. Under dlv the
# test binary is invoked directly, so the flag carries its -test. prefix.
verbose := $(if $(V),-v)
dlvverbose := $(if $(V),-test.v)

# Benchmarks must not run the test suite, and
#  the test suite must not run benchmarks;
# so each is filtered out of the other's invocation.
testflags  := $(verbose) \
  -run '$(run)'          \
  -count '$(count)'      \
  $(extra)
benchflags := $(verbose) \
  -run '^$$'             \
  -bench '$(bench)'      \
  -benchtime '$(time)'   \
  -count '$(count)'      \
  -benchmem              \
  $(extra)

# benchstat needs its own repetition count, and a run that carried both
# would silently keep only the last one.
statflags := $(verbose) \
  -run '^$$'            \
  -bench '$(bench)'     \
  -benchtime '$(time)'  \
  -count '$(stats)'     \
  -benchmem             \
  $(extra)

.PHONY: all test race cover report bench bench-race clean
.PHONY: prof cpu mem mutex block trace flame graph top list peek view
.PHONY: escape asm bin stat save
.PHONY: debug debug-test debug-bench debug-exec debug-attach debug-core
.PHONY: ci-cover ci-bench-new ci-bench-compare ci-install-go

all: test

test:
	@echo
	@echo test $(modimp)
	@echo
	go test $(testflags) $(package)

race:
	@echo
	@echo test $(modimp) with race detector
	@echo
	go test -race $(testflags) $(package)

cover: $(profile)

$(profile): | $(output)
	@echo
	@echo cover $(modimp)
	@echo
	go test -coverprofile=$@ -covermode=atomic $(testflags) $(package)
	@go tool cover -func=$@ | tail -n 1

report: $(report)

$(report): $(profile)
	go tool cover -html=$< -o $@
	@echo $@

bench:
	@echo
	@echo bench $(modimp)
	@echo
	go test $(benchflags) $(package)

bench-race:
	@echo
	@echo bench $(modimp) with race detector
	@echo
	go test -race $(benchflags) $(package)

# Profiling.
#
# Each profile is a file target that depends on the package sources, so a
# profile is regenerated after an edit and reused otherwise. Delete one, or
# run clean, to force a fresh measurement without editing anything.
#
# Every profile is taken from a benchmark run, and every benchmark override
# applies: BENCH selects what is measured and TIME how long for. A profile
# of the default '.' benchmark measures the whole suite at once, which is
# the right first look; narrow it with BENCH once a target is in view.
#
# Contention and allocation profiling perturb what they measure, so each
# rate lives in its own run rather than being folded into the CPU profile.

prof: cpu mem mutex block

cpu: $(cpuprof)

$(cpuprof): $(sources) | $(output)
	@echo
	@echo cpu profile $(modimp)
	@echo
	go test -o $(binary) -cpuprofile=$@ $(benchflags) $(single)
	@echo $@

mem: $(memprof)

$(memprof): $(sources) | $(output)
	@echo
	@echo allocation profile $(modimp)
	@echo
	go test -o $(binary) -memprofile=$@ -memprofilerate=$(memrate) $(benchflags) $(single)
	@echo $@

mutex: $(mutexprof)

$(mutexprof): $(sources) | $(output)
	@echo
	@echo mutex profile $(modimp)
	@echo
	go test -o $(binary) -mutexprofile=$@ -mutexprofilefraction=$(mutexrate) $(benchflags) $(single)
	@echo $@

block: $(blockprof)

$(blockprof): $(sources) | $(output)
	@echo
	@echo block profile $(modimp)
	@echo
	go test -o $(binary) -blockprofile=$@ -blockprofilerate=$(blockrate) $(benchflags) $(single)
	@echo $@

trace: $(traceout)

$(traceout): $(sources) | $(output)
	@echo
	@echo execution trace $(modimp)
	@echo
	go test -o $(binary) -trace=$@ $(benchflags) $(single)
	@echo $@

# Flame graph. Width is time spent, the stack reads caller below callee, and
# the widest tower is where the time went. Served rather than written: the
# graph is only useful if you can click into it.
flame: $(cpuprof) $(binary)
	@echo
	@echo http://$(addr)/ui/flamegraph
	@echo
	go tool pprof -http=$(addr) $(binary) $<

# Call graph, as a file, for when the answer needs to outlive the browser
# tab. Requires graphviz.
graph: $(callgraph)

$(callgraph): $(cpuprof) $(binary)
	go tool pprof -svg -output=$@ $(binary) $<
	@echo $@

# The two answers worth having before opening anything: where the time went,
# and which lines of one function spent it.
top: $(cpuprof) $(binary)
	go tool pprof -top -nodecount=$(nodes) $(binary) $<

list: $(cpuprof) $(binary)
	go tool pprof -list '$(func)' $(binary) $<

peek: $(cpuprof) $(binary)
	go tool pprof -peek '$(func)' $(binary) $<

view: $(traceout)
	go tool trace $<

# Compiler diagnostics. For a library that claims to allocate nothing, the
# escape analysis is the claim's proof, and it costs a single build. It is
# verbose, because it reports every generic instantiation the package pulls
# in as well as its own; M lowers the detail, and grep narrows it:
#
#   make escape M=1
#   make escape | grep broker.go
escape:
	@echo
	@echo escape analysis $(modimp)
	@echo
	go build -gcflags='-m=$(mlevel)' -o /dev/null $(single) 2>&1

asm:
	@echo
	@echo assembly $(modimp)
	@echo
	go build -gcflags='-S' -o /dev/null $(single) 2>&1

# The test binary, which pprof symbolizes against and dlv executes.
bin: $(binary)

$(binary): $(sources) | $(output)
	go test -c -o $@ $(single)
	@echo $@

# A benchmark result is a distribution, not a number. save records the
# current one; stat measures again and reports whether the difference is
# larger than the noise. Requires benchstat:
#
#   go install golang.org/x/perf/cmd/benchstat@latest
save: | $(output)
	@echo
	@echo baseline $(modimp)
	@echo
	go test $(statflags) $(package) | tee $(baseline)
	@echo $(baseline)

stat: | $(output)
	@echo
	@echo compare $(modimp) against baseline
	@echo
	@test -s $(baseline) || { echo 'no baseline: run make save first' >&2; exit 1; }
	@go test $(statflags) $(package) > $(current)
	benchstat $(baseline) $(current)

# Debugging.
#
# Delve builds without optimization or inlining, so a session shows the code
# as written. That also means it is not the code that runs in production:
# use it to answer what happened, not how fast.
#
# Set HEADLESS to any value to listen on DLVPORT instead of prompting, which
# is how an editor or a second terminal attaches:
#
#   make debug-test HEADLESS=1
#   dlv connect :2345

debug:
	dlv $(dlvflags) debug $(single)

debug-test:
	dlv $(dlvflags) test $(single) -- -test.run '$(run)' -test.count '$(count)' $(dlvverbose)

debug-bench:
	dlv $(dlvflags) test $(single) -- \
	  -test.run '^$$'                 \
	  -test.bench '$(bench)'          \
	  -test.benchtime '$(time)'       \
	  -test.count '$(count)'          \
	  $(dlvverbose)

debug-exec: $(binary)
	dlv $(dlvflags) exec $< -- -test.run '$(run)' $(dlvverbose)

# PID is required: make debug-attach PID=1234
debug-attach:
	@test -n '$(PID)' || { echo 'no PID: make debug-attach PID=<pid>' >&2; exit 1; }
	dlv $(dlvflags) attach $(PID)

# CORE is required: make debug-core CORE=/tmp/core.1234
debug-core:
	@test -n '$(CORE)' || { echo 'no CORE: make debug-core CORE=<file>' >&2; exit 1; }
	dlv $(dlvflags) core $(binary) $(CORE)

# CI targets.
#
# ci-install-go downloads and installs the Go toolchain version specified in
# go.mod directly from dl.google.com, which supports pre-release versions
# (e.g., rc, beta) that actions/setup-go cannot resolve.
# Assumes linux/amd64; set GOARCH and GOOS to override.
# After running, add /usr/local/go/bin to PATH:
#   export PATH=/usr/local/go/bin:$PATH
#
# ci-cover runs tests with coverage and enforces a 100% statement threshold.
# ci-bench-new records benchmark results for the current commit.
# ci-bench-compare diffs the current results against a saved baseline; it
# warns on regressions but does not fail the build, because benchstat exits
# 0 regardless and noise-only deltas are expected.
#
# THRESHOLD overrides the coverage floor:  make ci-cover THRESHOLD=90

goarch    := $(or $(GOARCH),amd64)
goos      := $(or $(GOOS),linux)
threshold := $(or $(THRESHOLD),100)

ci-install-go:
	@goversion=$$(sed -n 's/^go //p' $(moddir)/go.mod); \
	  echo "Installing go$${goversion} ($(goos)/$(goarch))..." >&2; \
	  curl -fsSL "https://dl.google.com/go/go$${goversion}.$(goos)-$(goarch).tar.gz" \
	    | sudo tar -C /usr/local -xz
	@echo "Installing benchstat..." >&2; \
	  /usr/local/go/bin/go install golang.org/x/perf/cmd/benchstat@latest
	@goroot=$$(go env GOROOT 2>/dev/null || echo /usr/local/go); \
	  gopath=$$(go env GOPATH 2>/dev/null || echo $${HOME}/go); \
	  echo "$${goroot}/bin"; \
	  echo "$${gopath}/bin"

ci-cover: | $(output)
	@echo
	@echo ci cover $(modimp)
	@echo
	go test -coverprofile=$(profile) -covermode=atomic -race $(package)
	@pct=$$(go tool cover -func=$(profile) | awk '/^total:/{gsub(/%/,"",$$3); print $$3}'); \
	  echo "Total coverage: $${pct}%"; \
	  if awk "BEGIN { exit !($${pct} < $(threshold)) }"; then \
	    echo "error: coverage $${pct}% is below threshold $(threshold)%" >&2; \
	    exit 1; \
	  fi

ci-bench-new: | $(output)
	@echo
	@echo ci bench $(modimp)
	@echo
	go test $(statflags) $(package) | tee $(current)

ci-bench-compare: | $(output)
	@echo
	@echo ci compare $(modimp)
	@echo
	@if [ -s $(baseline) ]; then \
	  benchstat $(baseline) $(current); \
	else \
	  echo 'no baseline: skipping regression comparison'; \
	fi

$(output):
	@mkdir -p $@

clean:
	@echo
	@echo clean $(modimp)
	@echo
	go clean -testcache
	go clean -i $(modimp)
	rm -rf $(output)

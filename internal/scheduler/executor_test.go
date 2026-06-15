package scheduler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"market-intel/internal/store"
)

// --- ShellExecutor ---

func TestShellExecutorSuccess(t *testing.T) {
	e := ShellExecutor{}
	rec := e.Run(store.Job{ID: "j1", Payload: "echo hello"})
	if rec.Status != "ok" {
		t.Fatalf("expected ok, got %q (output: %q)", rec.Status, rec.Output)
	}
	if !strings.Contains(rec.Output, "hello") {
		t.Errorf("expected 'hello' in output, got %q", rec.Output)
	}
}

func TestShellExecutorFailure(t *testing.T) {
	e := ShellExecutor{}
	rec := e.Run(store.Job{ID: "j2", Payload: "exit 42"})
	if rec.Status != "error" {
		t.Fatalf("expected error status, got %q", rec.Status)
	}
	if rec.ExitCode != 42 {
		t.Errorf("expected exit code 42, got %d", rec.ExitCode)
	}
}

func TestShellExecutorTimeout(t *testing.T) {
	e := ShellExecutor{Timeout: 100 * time.Millisecond}
	rec := e.Run(store.Job{ID: "j3", Payload: "sleep 10"})
	if rec.Status != "error" {
		t.Fatalf("expected error on timeout, got %q", rec.Status)
	}
	if !strings.Contains(rec.Output, "[timeout]") {
		t.Errorf("expected [timeout] in output, got %q", rec.Output)
	}
}

func TestShellExecutorSetsTimestamps(t *testing.T) {
	before := time.Now()
	e := ShellExecutor{}
	rec := e.Run(store.Job{ID: "j4", Payload: "echo ts"})
	after := time.Now()
	if rec.StartedAt.Before(before) || rec.StartedAt.After(after) {
		t.Errorf("StartedAt %v out of range [%v, %v]", rec.StartedAt, before, after)
	}
	if rec.FinishedAt.Before(rec.StartedAt) {
		t.Errorf("FinishedAt %v before StartedAt %v", rec.FinishedAt, rec.StartedAt)
	}
}

// --- HTTPExecutor ---

func TestHTTPExecutorSuccess(t *testing.T) {
	var gotMethod, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	e := HTTPExecutor{}
	rec := e.Run(store.Job{ID: "h1", Name: "test-job", Payload: srv.URL})
	if rec.Status != "ok" {
		t.Fatalf("expected ok, got %q (output: %q)", rec.Status, rec.Output)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("expected POST, got %q", gotMethod)
	}
	if !strings.Contains(gotContentType, "application/json") {
		t.Errorf("expected application/json content-type, got %q", gotContentType)
	}
	if rec.ExitCode != http.StatusOK {
		t.Errorf("expected ExitCode 200, got %d", rec.ExitCode)
	}
}

func TestHTTPExecutorNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	e := HTTPExecutor{}
	rec := e.Run(store.Job{ID: "h2", Payload: srv.URL})
	if rec.Status != "error" {
		t.Errorf("expected error for 500, got %q", rec.Status)
	}
	if rec.ExitCode != 500 {
		t.Errorf("expected ExitCode 500, got %d", rec.ExitCode)
	}
}

func TestHTTPExecutorBadURL(t *testing.T) {
	e := HTTPExecutor{}
	rec := e.Run(store.Job{ID: "h3", Payload: "http://127.0.0.1:1"}) // nothing listening
	if rec.Status != "error" {
		t.Errorf("expected error for unreachable URL, got %q", rec.Status)
	}
}

// --- KafkaExecutor ---

func TestKafkaExecutorNoBrokers(t *testing.T) {
	e := KafkaExecutor{} // no brokers → skip
	rec := e.Run(store.Job{ID: "k1", Payload: "market.quotes:hello"})
	if rec.Status != "ok" {
		t.Errorf("expected ok (noop skip), got %q", rec.Status)
	}
	if !strings.Contains(rec.Output, "no brokers") {
		t.Errorf("expected 'no brokers' in output, got %q", rec.Output)
	}
}

func TestKafkaExecutorBadPayload(t *testing.T) {
	e := KafkaExecutor{}
	rec := e.Run(store.Job{ID: "k2", Payload: "no-colon-here"})
	if rec.Status != "error" {
		t.Errorf("expected error for bad payload, got %q", rec.Status)
	}
}

// --- Dispatcher ---

func TestDispatcherShell(t *testing.T) {
	d := NewDispatcher()
	rec := d.Run(store.Job{ID: "d1", Executor: "shell", Payload: "echo dispatch"})
	if rec.Status != "ok" {
		t.Errorf("expected ok, got %q", rec.Status)
	}
}

func TestDispatcherUnknown(t *testing.T) {
	d := NewDispatcher()
	rec := d.Run(store.Job{ID: "d2", Executor: "grpc"})
	if rec.Status != "error" {
		t.Errorf("expected error for unknown executor, got %q", rec.Status)
	}
	if !strings.Contains(rec.Output, "unknown executor") {
		t.Errorf("expected 'unknown executor' in output, got %q", rec.Output)
	}
}

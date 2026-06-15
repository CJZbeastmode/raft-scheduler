package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"market-intel/internal/store"
)

const defaultShellTimeout = 10 * time.Minute

// ShellExecutor runs job.Payload as a shell command and captures stdout+stderr.
type ShellExecutor struct {
	Timeout time.Duration // 0 → defaultShellTimeout
}

func (e ShellExecutor) Run(j store.Job) store.RunRecord {
	timeout := e.Timeout
	if timeout == 0 {
		timeout = defaultShellTimeout
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", j.Payload)
	out, err := cmd.CombinedOutput()

	rec := store.RunRecord{
		JobID:      j.ID,
		StartedAt:  started,
		FinishedAt: time.Now(),
		Output:     string(out),
	}
	if err == nil {
		rec.Status = "ok"
	} else {
		rec.Status = "error"
		if ctx.Err() == context.DeadlineExceeded {
			rec.Output += "\n[timeout]"
		}
		if ee, ok := err.(*exec.ExitError); ok {
			rec.ExitCode = ee.ExitCode()
		}
	}
	return rec
}

// HTTPExecutor POSTs to job.Payload (a URL) with a JSON envelope containing job metadata.
type HTTPExecutor struct {
	Client *http.Client // nil → default 30s client
}

type httpEnvelope struct {
	JobID       string    `json:"job_id"`
	JobName     string    `json:"job_name"`
	TriggeredAt time.Time `json:"triggered_at"`
}

func (e HTTPExecutor) Run(j store.Job) store.RunRecord {
	client := e.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	started := time.Now()
	body, _ := json.Marshal(httpEnvelope{
		JobID:       j.ID,
		JobName:     j.Name,
		TriggeredAt: started,
	})
	rec := store.RunRecord{JobID: j.ID, StartedAt: started}

	resp, err := client.Post(j.Payload, "application/json", bytes.NewReader(body))
	rec.FinishedAt = time.Now()
	if err != nil {
		rec.Status = "error"
		rec.Output = err.Error()
		return rec
	}
	defer resp.Body.Close()
	rec.ExitCode = resp.StatusCode
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		rec.Status = "ok"
	} else {
		rec.Status = "error"
		rec.Output = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	return rec
}

// KafkaExecutor publishes to a Redpanda/Kafka topic.
// Payload format: "topic:message"
// Full kafka-go wiring added in Session 5. With no brokers configured the call is logged and skipped.
type KafkaExecutor struct {
	Brokers []string
}

func (e KafkaExecutor) Run(j store.Job) store.RunRecord {
	started := time.Now()
	parts := strings.SplitN(j.Payload, ":", 2)
	if len(parts) != 2 {
		return store.RunRecord{
			JobID: j.ID, StartedAt: started, FinishedAt: time.Now(),
			Status: "error", Output: `kafka payload must be "topic:message"`,
		}
	}
	topic, message := parts[0], parts[1]
	if len(e.Brokers) == 0 {
		log.Printf("[kafka] no brokers configured — skipping publish (topic=%s msg=%s)", topic, message)
		return store.RunRecord{
			JobID: j.ID, StartedAt: started, FinishedAt: time.Now(),
			Status: "ok", Output: fmt.Sprintf("skipped: no brokers (topic=%s)", topic),
		}
	}
	// TODO Session 5: segmentio/kafka-go producer
	_ = message
	return store.RunRecord{
		JobID: j.ID, StartedAt: started, FinishedAt: time.Now(),
		Status: "error", Output: "kafka executor not yet implemented",
	}
}

// Dispatcher routes each job to the executor named by job.Executor.
// Implements the Executor interface — pass it to scheduler.Make.
type Dispatcher struct {
	m map[string]Executor
}

// NewDispatcher returns a Dispatcher pre-loaded with shell/http/kafka executors.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{m: map[string]Executor{
		"shell": ShellExecutor{},
		"http":  HTTPExecutor{},
		"kafka": KafkaExecutor{},
	}}
}

func (d *Dispatcher) Run(j store.Job) store.RunRecord {
	ex, ok := d.m[j.Executor]
	if !ok {
		return store.RunRecord{
			JobID: j.ID, StartedAt: time.Now(), FinishedAt: time.Now(),
			Status: "error", Output: "unknown executor type: " + j.Executor,
		}
	}
	return ex.Run(j)
}

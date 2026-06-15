package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"market-intel/internal/store"
)

// --- fakes ---

type fakeStore struct {
	mu   sync.Mutex
	jobs map[string]store.Job
	runs map[string][]store.RunRecord
	err  error // if non-nil, Submit returns this
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		jobs: make(map[string]store.Job),
		runs: make(map[string][]store.RunRecord),
	}
}

func (f *fakeStore) Submit(cmd store.Cmd) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Type {
	case store.CmdUpsert:
		f.jobs[cmd.Job.ID] = cmd.Job
	case store.CmdDelete:
		delete(f.jobs, cmd.ID)
		delete(f.runs, cmd.ID)
	}
	return nil
}

func (f *fakeStore) GetJob(id string) (store.Job, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	return j, ok
}

func (f *fakeStore) ListJobs() []store.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		out = append(out, j)
	}
	return out
}

func (f *fakeStore) GetRuns(id string) []store.RunRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.RunRecord(nil), f.runs[id]...)
}

type fakeScheduler struct {
	mu       sync.Mutex
	fired    []string
	knownIDs map[string]bool
}

func (f *fakeScheduler) FireNow(id string) error {
	if !f.knownIDs[id] {
		return fmt.Errorf("job %q not found", id)
	}
	f.mu.Lock()
	f.fired = append(f.fired, id)
	f.mu.Unlock()
	return nil
}

type fakeLeader struct{ leader bool; term int }

func (f fakeLeader) GetState() (int, bool) { return f.term, f.leader }

func newTestServer(st *fakeStore, sc *fakeScheduler, lc fakeLeader) *Server {
	return &Server{st: st, sc: sc, lc: lc}
}

// --- helpers ---

func post(t *testing.T, h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(rec.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, rec.Body)
	}
}

// --- tests ---

func TestCreateJobOK(t *testing.T) {
	st := newFakeStore()
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{leader: true, term: 1})

	rec := post(t, s.createJob, `{"name":"quotes","cron_expr":"* * * * *","executor":"shell","payload":"echo hi","catchup_policy":"skip"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body)
	}

	var job store.Job
	decodeBody(t, rec, &job)
	if job.ID == "" {
		t.Error("expected auto-generated ID")
	}
	if job.NextRun.IsZero() {
		t.Error("expected NextRun computed server-side")
	}
	if job.NextRun.Before(time.Now().Add(-time.Second)) {
		t.Errorf("NextRun %v should be in the future", job.NextRun)
	}
	// also present in store
	if _, ok := st.GetJob(job.ID); !ok {
		t.Error("job not found in store after create")
	}
}

func TestCreateJobWithExplicitID(t *testing.T) {
	st := newFakeStore()
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{leader: true})

	rec := post(t, s.createJob, `{"id":"my-job","name":"x","cron_expr":"0 * * * *","executor":"http","payload":"http://x"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body)
	}
	if _, ok := st.GetJob("my-job"); !ok {
		t.Error("expected job with explicit ID 'my-job' in store")
	}
}

func TestCreateJobBadCron(t *testing.T) {
	st := newFakeStore()
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{})

	rec := post(t, s.createJob, `{"cron_expr":"not-a-cron","executor":"shell","payload":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad cron, got %d", rec.Code)
	}
}

func TestCreateJobNotLeader(t *testing.T) {
	st := newFakeStore()
	st.err = store.ErrNotLeader
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{})

	rec := post(t, s.createJob, `{"cron_expr":"* * * * *","executor":"shell","payload":"x"}`)
	if rec.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d", rec.Code)
	}
}

func TestListJobsEmpty(t *testing.T) {
	s := newTestServer(newFakeStore(), &fakeScheduler{}, fakeLeader{})
	req := httptest.NewRequest("GET", "/jobs", nil)
	rec := httptest.NewRecorder()
	s.listJobs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var result map[string][]store.Job
	decodeBody(t, rec, &result)
	if len(result["jobs"]) != 0 {
		t.Errorf("expected empty jobs, got %d", len(result["jobs"]))
	}
}

func TestListJobsReturnsAll(t *testing.T) {
	st := newFakeStore()
	st.jobs["a"] = store.Job{ID: "a"}
	st.jobs["b"] = store.Job{ID: "b"}
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{})

	req := httptest.NewRequest("GET", "/jobs", nil)
	rec := httptest.NewRecorder()
	s.listJobs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var result map[string][]store.Job
	decodeBody(t, rec, &result)
	if len(result["jobs"]) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(result["jobs"]))
	}
}

func TestGetJobFound(t *testing.T) {
	st := newFakeStore()
	st.jobs["abc"] = store.Job{ID: "abc", Name: "test"}
	st.runs["abc"] = []store.RunRecord{{JobID: "abc", Status: "ok"}}
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{})

	req := httptest.NewRequest("GET", "/jobs/abc", nil)
	req.SetPathValue("id", "abc")
	rec := httptest.NewRecorder()
	s.getJob(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var detail jobDetail
	decodeBody(t, rec, &detail)
	if detail.Job.ID != "abc" {
		t.Errorf("expected ID 'abc', got %q", detail.Job.ID)
	}
	if len(detail.Runs) != 1 {
		t.Errorf("expected 1 run, got %d", len(detail.Runs))
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := newTestServer(newFakeStore(), &fakeScheduler{}, fakeLeader{})
	req := httptest.NewRequest("GET", "/jobs/nope", nil)
	req.SetPathValue("id", "nope")
	rec := httptest.NewRecorder()
	s.getJob(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestDeleteJobOK(t *testing.T) {
	st := newFakeStore()
	st.jobs["del-me"] = store.Job{ID: "del-me"}
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{})

	req := httptest.NewRequest("DELETE", "/jobs/del-me", nil)
	req.SetPathValue("id", "del-me")
	rec := httptest.NewRecorder()
	s.deleteJob(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
	if _, ok := st.GetJob("del-me"); ok {
		t.Error("job still in store after delete")
	}
}

func TestDeleteJobNotFound(t *testing.T) {
	s := newTestServer(newFakeStore(), &fakeScheduler{}, fakeLeader{})
	req := httptest.NewRequest("DELETE", "/jobs/ghost", nil)
	req.SetPathValue("id", "ghost")
	rec := httptest.NewRecorder()
	s.deleteJob(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestTriggerJobOK(t *testing.T) {
	sc := &fakeScheduler{knownIDs: map[string]bool{"fire-me": true}}
	s := newTestServer(newFakeStore(), sc, fakeLeader{})

	req := httptest.NewRequest("POST", "/jobs/fire-me/trigger", nil)
	req.SetPathValue("id", "fire-me")
	rec := httptest.NewRecorder()
	s.triggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.fired) != 1 || sc.fired[0] != "fire-me" {
		t.Errorf("expected FireNow called with 'fire-me', got %v", sc.fired)
	}
}

func TestTriggerJobNotFound(t *testing.T) {
	s := newTestServer(newFakeStore(), &fakeScheduler{knownIDs: map[string]bool{}}, fakeLeader{})
	req := httptest.NewRequest("POST", "/jobs/ghost/trigger", nil)
	req.SetPathValue("id", "ghost")
	rec := httptest.NewRecorder()
	s.triggerJob(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestClusterStatusLeader(t *testing.T) {
	s := newTestServer(newFakeStore(), &fakeScheduler{}, fakeLeader{leader: true, term: 3})
	req := httptest.NewRequest("GET", "/cluster", nil)
	rec := httptest.NewRecorder()
	s.clusterStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp clusterStatusResp
	decodeBody(t, rec, &resp)
	if !resp.IsLeader {
		t.Error("expected is_leader=true")
	}
	if resp.Term != 3 {
		t.Errorf("expected term 3, got %d", resp.Term)
	}
}

func TestClusterStatusFollower(t *testing.T) {
	s := newTestServer(newFakeStore(), &fakeScheduler{}, fakeLeader{leader: false, term: 2})
	req := httptest.NewRequest("GET", "/cluster", nil)
	rec := httptest.NewRecorder()
	s.clusterStatus(rec, req)
	var resp clusterStatusResp
	decodeBody(t, rec, &resp)
	if resp.IsLeader {
		t.Error("expected is_leader=false for follower")
	}
}

func TestCreateJobDefaultCatchupPolicy(t *testing.T) {
	st := newFakeStore()
	s := newTestServer(st, &fakeScheduler{}, fakeLeader{})

	rec := post(t, s.createJob, `{"cron_expr":"* * * * *","executor":"shell","payload":"x"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body)
	}
	var job store.Job
	decodeBody(t, rec, &job)
	if job.CatchupPolicy != "skip" {
		t.Errorf("expected default catchup_policy 'skip', got %q", job.CatchupPolicy)
	}
}

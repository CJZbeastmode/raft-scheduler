package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"market-intel/internal/store"
	"market-intel/pkg/cron"
)

// createJobReq is the JSON body for POST /jobs.
type createJobReq struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	CronExpr      string `json:"cron_expr"`
	Executor      string `json:"executor"`
	Payload       string `json:"payload"`
	CatchupPolicy string `json:"catchup_policy"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	expr, err := cron.Parse(req.CronExpr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cron_expr: "+err.Error())
		return
	}
	if req.ID == "" {
		req.ID = generateID()
	}
	if req.CatchupPolicy == "" {
		req.CatchupPolicy = "skip"
	}

	job := store.Job{
		ID:            req.ID,
		Name:          req.Name,
		CronExpr:      req.CronExpr,
		Executor:      req.Executor,
		Payload:       req.Payload,
		CatchupPolicy: req.CatchupPolicy,
		NextRun:       expr.Next(time.Now()),
		CreatedAt:     time.Now(),
	}
	if err := s.st.Submit(store.Cmd{Type: store.CmdUpsert, ReqID: generateID(), Job: job}); err != nil {
		if err == store.ErrNotLeader {
			writeError(w, http.StatusConflict, "not the leader — retry against the leader node")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs := s.st.ListJobs()
	if jobs == nil {
		jobs = []store.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

type jobDetail struct {
	Job  store.Job         `json:"job"`
	Runs []store.RunRecord `json:"runs"`
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, ok := s.st.GetJob(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found: "+id)
		return
	}
	runs := s.st.GetRuns(id)
	if runs == nil {
		runs = []store.RunRecord{}
	}
	writeJSON(w, http.StatusOK, jobDetail{Job: j, Runs: runs})
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.st.GetJob(id); !ok {
		writeError(w, http.StatusNotFound, "job not found: "+id)
		return
	}
	if err := s.st.Submit(store.Cmd{Type: store.CmdDelete, ReqID: generateID(), ID: id}); err != nil {
		if err == store.ErrNotLeader {
			writeError(w, http.StatusConflict, "not the leader")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) triggerJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.sc.FireNow(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "triggered", "job_id": id})
}

type clusterStatusResp struct {
	Term     int  `json:"term"`
	IsLeader bool `json:"is_leader"`
}

func (s *Server) clusterStatus(w http.ResponseWriter, r *http.Request) {
	term, isLeader := s.lc.GetState()
	writeJSON(w, http.StatusOK, clusterStatusResp{Term: term, IsLeader: isLeader})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

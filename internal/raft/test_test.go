package raft

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

const (
	electionWait  = 3 * time.Second
	agreementWait = 2 * time.Second
	pollInterval  = 20 * time.Millisecond
)

// --- cluster harness ---

type testCluster struct {
	t          *testing.T
	n          int
	addrs      []string
	rafts      []*Raft
	listeners  []net.Listener
	persisters []*Persister
	applyChs   []chan ApplyMsg

	mu      sync.Mutex
	applied [][]ApplyMsg
	alive   []bool
}

func makeCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{
		t:          t,
		n:          n,
		addrs:      make([]string, n),
		rafts:      make([]*Raft, n),
		listeners:  make([]net.Listener, n),
		persisters: make([]*Persister, n),
		applyChs:   make([]chan ApplyMsg, n),
		applied:    make([][]ApplyMsg, n),
		alive:      make([]bool, n),
	}

	// allocate all listeners first so every address is known before any Raft node starts
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		c.listeners[i] = ln
		c.addrs[i] = ln.Addr().String()
	}

	peers := c.buildPeers()

	for i := 0; i < n; i++ {
		c.persisters[i] = MakePersister()
		c.applyChs[i] = make(chan ApplyMsg, 256)
		c.rafts[i] = Make(peers, i, c.persisters[i], c.applyChs[i])
		c.alive[i] = true

		srv := rpc.NewServer()
		if err := srv.RegisterName("Raft", &RPCAdapter{Rf: c.rafts[i]}); err != nil {
			t.Fatalf("register node %d: %v", i, err)
		}
		go srv.Accept(c.listeners[i])
		go c.drainApplied(i)
	}

	return c
}

func (c *testCluster) buildPeers() []*Peer {
	peers := make([]*Peer, c.n)
	for i, addr := range c.addrs {
		peers[i] = NewPeer(addr)
	}
	return peers
}

// drainApplied collects CommandValid messages from node i's applyCh.
func (c *testCluster) drainApplied(i int) {
	for msg := range c.applyChs[i] {
		if msg.CommandValid {
			c.mu.Lock()
			c.applied[i] = append(c.applied[i], msg)
			c.mu.Unlock()
		}
	}
}

func (c *testCluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.rafts[i].Kill()
		c.listeners[i].Close()
	}
}

// kill stops node i: halts goroutines and closes its TCP listener.
func (c *testCluster) kill(i int) {
	c.mu.Lock()
	c.alive[i] = false
	c.mu.Unlock()
	c.rafts[i].Kill()
	c.listeners[i].Close()
}

// waitForLeader blocks until an alive node reports isLeader, returns (index, term).
func (c *testCluster) waitForLeader() (int, int) {
	c.t.Helper()
	deadline := time.Now().Add(electionWait)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			alive := c.alive[i]
			c.mu.Unlock()
			if !alive {
				continue
			}
			term, isLeader := c.rafts[i].GetState()
			if isLeader {
				return i, term
			}
		}
		time.Sleep(pollInterval)
	}
	c.t.Fatalf("no leader elected within %v", electionWait)
	return -1, -1
}

// leadersByTerm returns a map of term → []node for all alive leaders.
func (c *testCluster) leadersByTerm() map[int][]int {
	c.mu.Lock()
	alive := append([]bool(nil), c.alive...)
	c.mu.Unlock()

	out := map[int][]int{}
	for i := 0; i < c.n; i++ {
		if !alive[i] {
			continue
		}
		term, isLeader := c.rafts[i].GetState()
		if isLeader {
			out[term] = append(out[term], i)
		}
	}
	return out
}

// nApplied counts how many nodes have applied the entry at index, and returns its command.
func (c *testCluster) nApplied(index int) (int, interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd interface{}
	for i := 0; i < c.n; i++ {
		for _, msg := range c.applied[i] {
			if msg.CommandIndex == index {
				count++
				cmd = msg.Command
				break
			}
		}
	}
	return count, cmd
}

// waitForCommit polls until at least `need` nodes have applied `index`, returns the command.
func (c *testCluster) waitForCommit(index, need int) interface{} {
	c.t.Helper()
	deadline := time.Now().Add(agreementWait)
	for time.Now().Before(deadline) {
		n, cmd := c.nApplied(index)
		if n >= need {
			return cmd
		}
		time.Sleep(pollInterval)
	}
	n, _ := c.nApplied(index)
	c.t.Fatalf("index %d: %d/%d nodes applied after %v", index, n, need, agreementWait)
	return nil
}

// --- tests ---

func TestInitialElection(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leader, term := c.waitForLeader()
	if leader < 0 {
		t.Fatal("no leader")
	}
	if term < 1 {
		t.Fatalf("term must be >= 1, got %d", term)
	}
}

func TestSingleLeaderPerTerm(t *testing.T) {
	c := makeCluster(t, 5)
	defer c.cleanup()

	c.waitForLeader()
	time.Sleep(300 * time.Millisecond) // let any split votes resolve

	for term, nodes := range c.leadersByTerm() {
		if len(nodes) > 1 {
			t.Errorf("term %d has %d simultaneous leaders: %v", term, len(nodes), nodes)
		}
	}
}

func TestTermNonDecreasing(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	c.waitForLeader()

	snapshot := make([]int, c.n)
	for i := 0; i < c.n; i++ {
		snapshot[i], _ = c.rafts[i].GetState()
	}

	time.Sleep(400 * time.Millisecond)

	for i := 0; i < c.n; i++ {
		term, _ := c.rafts[i].GetState()
		if term < snapshot[i] {
			t.Errorf("node %d: term went backwards %d → %d", i, snapshot[i], term)
		}
	}
}

func TestStartReturnsLeaderOnly(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leaderIdx, _ := c.waitForLeader()

	if _, _, ok := c.rafts[leaderIdx].Start(1); !ok {
		t.Errorf("Start on leader (node %d) returned isLeader=false", leaderIdx)
	}
	for i := 0; i < c.n; i++ {
		if i == leaderIdx {
			continue
		}
		if _, _, ok := c.rafts[i].Start(99); ok {
			t.Errorf("Start on follower (node %d) returned isLeader=true", i)
		}
	}
}

func TestBasicAgree(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leaderIdx, _ := c.waitForLeader()

	for cmd := 1; cmd <= 3; cmd++ {
		index, _, ok := c.rafts[leaderIdx].Start(cmd)
		if !ok {
			t.Fatalf("Start(cmd=%d) returned false on leader", cmd)
		}
		c.waitForCommit(index, c.n)
	}
}

func TestAgreementCommandPreserved(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leaderIdx, _ := c.waitForLeader()

	const want = 99999
	index, _, ok := c.rafts[leaderIdx].Start(want)
	if !ok {
		t.Fatal("Start returned false on leader")
	}
	got := c.waitForCommit(index, c.n)
	if got != want {
		t.Errorf("applied command = %v, want %v", got, want)
	}
}

func TestReElection(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leader1, term1 := c.waitForLeader()
	c.kill(leader1)

	leader2, term2 := c.waitForLeader()
	if leader2 == leader1 {
		t.Errorf("same node %d re-elected; expected a different leader", leader1)
	}
	if term2 <= term1 {
		t.Errorf("re-election term %d not higher than original term %d", term2, term1)
	}
}

func TestStatePersisted(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	c.waitForLeader()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < c.n; i++ {
		if len(c.persisters[i].ReadRaftState()) == 0 {
			t.Errorf("node %d has empty persisted state after election", i)
		}
	}
}

func TestConcurrentStarts(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leaderIdx, _ := c.waitForLeader()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var indices []int

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(cmd int) {
			defer wg.Done()
			idx, _, ok := c.rafts[leaderIdx].Start(cmd * 10)
			if ok {
				mu.Lock()
				indices = append(indices, idx)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	for _, idx := range indices {
		c.waitForCommit(idx, c.n)
	}
}

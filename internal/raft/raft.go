package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"encoding/gob"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*Peer			  // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor	int
	log			[]LogEntry
	lastIncludedIndex		int  // for snapshot
	lastIncludedTerm		int  // for snapshot

	// Volatile state
	commitIndex int				// Log exists in majority of servers, but not applied/executed
								// "last entry we know is safe to hand upstairs"
	lastApplied int				// Entry handled by layer above (KV store, application, etc.)
								// "last entry we handed upstairs"

	// Volatile state, only needed by leaders
	nextIndex 	[]int			// leader's guess at what index to send next to that follower, gradually fall back if not identical
								// "I think you need entries from here"
	matchIndex 	[]int			// leader's confirmed knowledge of how far a follower's log matches its own
								// "I know for certain you have up to here"

	// bookkeeping
	state		string  		// follower, candidate, leader
	lastHeartbeat time.Time

	// to verify that all servers applied the same commands in the same order
	applyCh chan ApplyMsg		// output pipe
	pendingSnapshot *ApplyMsg	// param to prevent multiple calls of applyCh
}

type LogEntry struct {
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).

	// Isolated get
	rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state == "leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).

	/*
	w := new(bytes.Buffer)				// create an in-memory byte buffer
	e := gob.NewEncoder(w)				// wrap it with an encoder
	e.Encode(rf.currentTerm)			// serialize currentTerm into the buffer
	e.Encode(rf.votedFor)				// serialize votedFor
	e.Encode(rf.log)					// serialize the entire log slice
	e.Encode(rf.lastIncludedIndex)		// serialize the last included index without snapshot
	e.Encode(rf.lastIncludedTerm)		// serialize the last included term without snapshot
	raftstate := w.Bytes()				// extract the raw bytes
	rf.persister.Save(raftstate, nil)	// hand to the persister (nil = no snapshot yet)
	*/

	// replace block with
	rf.persistWithSnapshot(rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)				// create an in-memory byte buffer
	e := gob.NewEncoder(w)				// wrap it with an encoder
	e.Encode(rf.currentTerm)			// serialize currentTerm into the buffer
	e.Encode(rf.votedFor)				// serialize votedFor
	e.Encode(rf.log)					// serialize the entire log slice
	e.Encode(rf.lastIncludedIndex)		// serialize the last included index without snapshot
	e.Encode(rf.lastIncludedTerm)		// serialize the last included term without snapshot
	raftstate := w.Bytes()				// extract the raw bytes
	rf.persister.Save(raftstate, snapshot)	// hand to the persister with snapshot
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
	
	// first boot, nothing saved yet — start fresh
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)			// wrap the raw bytes in a reader
	d := gob.NewDecoder(r)				// wrap with a decoder
	
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	// decode error — ignore, start fresh
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
			return
		}

	// restore the fields
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	// Lock to prevent snapshot concurrency
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// no snapshot if stale or already-snapshotted index
	if index <= rf.lastIncludedIndex { return }

	newLastIncludedTerm := rf.logTerm(index)
	// trim the log
	rf.log = rf.log[rf.logIndex(index):]
	rf.log[0].Command = nil 				// sentinel, term is preserved so command not needed
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = newLastIncludedTerm

	// Persist both raft state and the snapshot
    rf.persistWithSnapshot(snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term 			int
	VoteGranted		bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// 1, Stale candidate --> reject
	if args.Term < rf.currentTerm { return }

	// 2, higher than itself --> update and demote
	if args.Term > rf.currentTerm { 
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1 
		// !! Store in persistet
		rf.persist()
	}



	// 3, already voted --> reject
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		//rf.lastHeartbeat = time.Now() --> not needed
		// Already voted for a different candidate this term — reject.
		// Do NOT reset heartbeat here: a rejected candidate is not evidence
		// the cluster is making progress. Timer stays as-is so we remain
		// ready to start a legitimate election if needed.
		return 
	}

	// 4, check 
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.logTerm(lastIndex)
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
			// more up to date than cur		// exact up to date term, but whoever has more entries is more up-to-date. 
	if !logOk {return}	// reject if not satisfied

	// Grant vote
	reply.VoteGranted = true

	// Update heartbeat time and votedfor
	rf.votedFor = args.CandidateId
	// Store it in persistency
	rf.persist()
	rf.lastHeartbeat = time.Now()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only the leader accepts new commands
	if rf.state != "leader" {
		return -1, rf.currentTerm, false
	}

	// If not leader, append to own log immediately
	entry := LogEntry{
		Term: rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index := rf.lastLogIndex()
	term := rf.currentTerm
	// Persist log
	rf.persist()

	// Send heartbeats to inform the peers that it is the leader
	go rf.sendHeartbeats()
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		// NOT USED
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*Peer, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = "follower"
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// After restoring persisted state, these must be at least lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.electionTimeoutLoop()

	// start apply loop
	go rf.applyLoop()

	return rf
}


// Bridge the gap between commit and apply
// Send commited logs upward for application
// Background routine
func (rf *Raft) applyLoop() {
	// Loop until cut
	for !rf.killed() {
		// 10 ms gap between loops, background pause
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		// Send pending snapshot first if one exists
		// All snapshots are sent here, installsnapshot do not send it but rather archive it and let it to be sent here.
        if rf.pendingSnapshot != nil {
            msg := *rf.pendingSnapshot
            rf.pendingSnapshot = nil
            rf.mu.Unlock()
            rf.applyCh <- msg
            continue
        }

		// Check gap and move forward
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied ++ 

			// Safety check — should never happen but guards against index underflow
			if rf.lastApplied <= rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
				continue
			}

			msg := ApplyMsg {
				CommandValid: 	true,
				Command: 		rf.log[rf.logIndex(rf.lastApplied)].Command,
				CommandIndex: 	rf.lastApplied,
			}
			// Temp unlock to send msg
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}	
}


// Election timeout loop to start elections when necessary
func (rf *Raft) electionTimeoutLoop() {
	// Loop until the node is killed
	for !rf.killed() {
		// 250~400 ms random waiting time, higher to allow heartbeat to arrive first
		sleepDuration := time.Duration(250 + rand.Intn(150)) * time.Millisecond
		time.Sleep(sleepDuration)

		// Lock RF to prevent concurrency
		// Start RF operations
		rf.mu.Lock()
		// Start election if RF is not leader or not received heartbeat during sleeping --> leader suspicious dead / no leader
		if rf.state != "leader" && time.Since(rf.lastHeartbeat) >= sleepDuration {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = "candidate"		// change back to candidate
	rf.currentTerm ++ 			// raise term
	rf.votedFor = rf.me			// vote for himself
	// Persistence
	rf.persist()
	votes := 1   				// votes count
	term := rf.currentTerm 		// snapshot term

	// loop through all peers and request vote
	for i := range rf.peers {
		// skip myself
		if i == rf.me {
			continue
		}

		lastIdx := rf.lastLogIndex()
		lastTerm := rf.logTerm(lastIdx)

		// wrap in go for parallel programming
		go func(peer int) {
			// construct requestVotes
			args := &RequestVoteArgs{
				Term: term,									// this term
				CandidateId: rf.me,							// himself
				LastLogIndex: lastIdx,			// last Log
				LastLogTerm: lastTerm,	// last Log Term
			}
			reply := &RequestVoteReply{}

			// call RPC
			ok := rf.peers[peer].Call("Raft.RequestVote", args, reply) 
			if !ok { return }

			// lock to prevent concurrency
			rf.mu.Lock()
			defer rf.mu.Unlock() // unlock automatically upon exit of function

			// Check stale data inbetween --> ignore (things might get updated during the loop, example: winning already)
			if rf.currentTerm != term || rf.state != "candidate" {
				return 
			}

			// See someone with higher term --> step down
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term		// update term
				rf.state = "follower"			// change back to follwer
				rf.votedFor = -1				// reinit votedFor
				// Persist
				rf.persist()
				return
			}

			// Increment vote if vote is granted
			if reply.VoteGranted {
				votes ++ 
				// if majority than win automatic
				if votes >= len(rf.peers)/2 + 1 {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}


func (rf *Raft) becomeLeader() {
    rf.state = "leader"
    // Initialize leader-only state
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for i := range rf.peers {
        rf.nextIndex[i] = rf.lastLogIndex() + 1 // start optimistic, then gradually fall back
        rf.matchIndex[i] = 0 // assume that we have confirmed nothing yet
    }
    go rf.heartbeatLoop()
}


func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		// check if still leader while doing this
		if rf.state != "leader" {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		
		// if not leader, send heartbeat to all
		rf.sendHeartbeats()
		// wait 100 ms before next round of heartbeat
		// shorter than election timeout -->
			// If heartbeats arrive slower than the timeout
			// followers will think the leader died and start unnecessary elections
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats() {
	// get current terms and info without concurrency
	rf.mu.Lock()
	term := rf.currentTerm			// Snapshot, but keep checking during the loop that it is leader
	LeaderId := rf.me
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	// loop to send heartbeat
	for i := range rf.peers {
		// skip himself
		if i == rf.me { continue }

		// parallel programming
		go func(peer int) {
			// Lock to prevent concurrency
			rf.mu.Lock()

			// Check whether is still leader
			if rf.state != "leader" {
				rf.mu.Unlock()
				return
			}

			// Follower is too far behind — send snapshot instead
			if rf.nextIndex[peer] <= rf.lastIncludedIndex {
				rf.sendSnapshot(peer)
				return
			}

			// fetch nextIndex --> leader's guess at what index to send next to that follower
			ni := rf.nextIndex[peer]
			// check prevLog
			prevLogIndex := ni - 1
			prevLogTerm := rf.logTerm(prevLogIndex)
			// make entries
			entries := make([]LogEntry, len(rf.log[rf.logIndex(ni):]))
			copy(entries, rf.log[rf.logIndex(ni):])
			rf.mu.Unlock()


			// Construct and call AppendEntries
			args := &AppendEntriesArgs{
				Term: term,
				LeaderId: LeaderId,
				PrevLogIndex: prevLogIndex,
                PrevLogTerm:  prevLogTerm,
                Entries:      entries,
                LeaderCommit: commitIndex,
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

			// false response --> no good, ignore (maybe network error/timeout/server unreachable)
			if !ok { return }

			// Lock to prevent concurrency
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Stale reply — ignore
            if rf.state != "leader" || rf.currentTerm != term { return }

			// Find out himself is stale --> demote
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.votedFor = -1
				// Persist
				rf.persist()
			}

			// On Success
			if reply.Success {
				// Update prevLogIndex and matchIndex

				// compute newly matches index
				newMatch := prevLogIndex + len(entries)

				// update matched index if necessary
				if newMatch > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatch
				}

				// set nextIndex
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1

				// update commit indices if majority has been reached
				rf.maybeAdvanceCommitIndex()
			} else { 	// On Failure
				// back up one and retry next heartbeat
				// more GPC calls
				/*
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer] --
				}
				*/

				// Use hint by the follower to avoid gradual try
				if reply.ConflictTerm == -1 {
					// Follower said "I don't even have that index"
					// Jump directly to where their log ends
					// No point sending anything before that
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					newIndex := -1
					// searches its own log backwards for the conflicting term.
					for j := rf.lastLogIndex(); j > rf.lastIncludedIndex; j-- {
						// stop at first found
						if rf.logTerm(j) == reply.ConflictTerm {
							newIndex = j + 1
							break
						}
					}

					if newIndex == -1 {
						// Leader doesn't have that term at all
       					// Follower has entries leader never had → overwrite from ConflictIndex
						rf.nextIndex[peer] = reply.ConflictIndex
					} else {
						// Leader has that term → start sending from just past it
						rf.nextIndex[peer] = newIndex
					}
				}

				// Safety floor — nextIndex can never go below 1 because index 0 is the sentinel entry that always exists on every server.
				if rf.nextIndex[peer] < 1 {
					rf.nextIndex[peer] = 1
				}
	
			}

		}(i)
	}
}

type AppendEntriesArgs struct {
	Term 		int				// current Term
	LeaderId	int				// heartbeat from node LeaderID
	PrevLogIndex	int			// Use Prev: because for consistency check during append/correction
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term 		int
	Success 	bool

	// For faster fallback only
	ConflictTerm  int  // term of the conflicting entry (-1 if log too short)
    ConflictIndex int  // first index of that conflicting term
}


// Heartbeat + replicate logs
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// init conflict term and index to avoid automatic init to 0
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// stale leader, ignore
	if args.Term < rf.currentTerm { return }

	// valid heartbeat - reset election timer
	rf.lastHeartbeat = time.Now()		// reset last heartbeat
	rf.state = "follower"				// reset to follower
	rf.currentTerm = args.Term			// update term
	// Persist
	rf.persist()
	reply.Term = rf.currentTerm

	// find out where the conflict term is and send/tell the leader

	// leader is behind our snapshot — this shouldn't happen normally
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// no entry at PrevLogIndex in the log --> reject
	if args.PrevLogIndex > rf.lastLogIndex() { 
		// for faster fallback only: 
		// set it in a way that shows it it too short
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return 
	}
	// entry at PrevLogIndex is wrong --> reject
	if rf.logTerm(args.PrevLogIndex) != args.PrevLogTerm { 
		// for faster fallback only:
		// update conflictTerm
		reply.ConflictTerm = rf.logTerm(args.PrevLogIndex)
		reply.ConflictIndex = args.PrevLogIndex
		// walk back to find first index of this conflicting term
		for reply.ConflictIndex > rf.lastIncludedIndex + 1 && rf.logTerm(reply.ConflictIndex-1) == reply.ConflictTerm {
			reply.ConflictIndex --
		}
		return 
	}

	// append new entries and correct existing wrong entries, to be identical to leader
	// loop through all entries
	for i, entry := range args.Entries {
		// look for current index
		idx := args.PrevLogIndex + 1 + i
			// curr = prev + 1
			// i -> position index
		
		// For past log --> check conflict
		// rf.lastLogIndex() + 1 is the length of log
		if idx < rf.lastLogIndex() + 1  {
			// Conflict --> Correction
			if rf.logTerm(idx) != entry.Term {
				rf.log = rf.log[:rf.logIndex(idx)]	// truncate from conflict point, and keep the logs before conflict
				rf.log = append(rf.log, args.Entries[i:]...)
				break // all corrected and appended, therefore break
			}
			// No conflict --> skip
			// else: do nothing
		} else {
			// Past end of our log 
			// Append all remaining entries from leader
			rf.log = append(rf.log, args.Entries[i:]...)
			break // all appended, therefore break
		}
	}
	// Persist log
	rf.persist()

	// Logic to set commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// New index after appending
		lastNewIndex := args.PrevLogIndex + len(args.Entries)

		// Clamp to what we actually have — leader may be ahead of what it sent us
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit  // leader is behind our new entries
		} else {
			rf.commitIndex = lastNewIndex		 // leader is ahead, cap at what we have
		}
	}

	reply.Success = true	// return true
}


// Check whether any index has been replicated on majority
func (rf *Raft) maybeAdvanceCommitIndex() {
	// Loop through all index above commited index
	for n := rf.commitIndex + 1; n <= rf.lastLogIndex(); n ++ {
		// Only commit entries from the current term 
		if rf.logTerm(n) != rf.currentTerm { continue }

		// Count how many peers have replicated up to n
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count ++
			}
		}

		// If majority --> commitIndex
		if count >= len(rf.peers) / 2 + 1 {
			rf.commitIndex = n
		}
	}
}


// Snapshot RPC
type InstallSnapshotArgs struct {
	Term				int
	LeaderId			int
	LastIncludedIndex 	int
	LastIncludedTerm	int
	Data				[]byte  // snapshot bytes
}

type InstallSnapshotReply struct {
	Term				int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// prevent concurrency snapshot store
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// stale leader -> reject
	if args.Term < rf.currentTerm { return }

	// valid message -> update term and reset timer
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = "follower"
	rf.lastHeartbeat = time.Now()

	// Snapshot is outdated — skip
	if args.LastIncludedIndex <= rf.lastIncludedIndex { return }

	// Trim log to match snpashot
	if args.LastIncludedIndex <= rf.lastLogIndex() {
		// keep entries past the snapshot
		/*
		Follower log: [S, A, B, C, D, E]   lastIncludedIndex=0
		Snapshot covers up to index 3 (C)
		After trim:   [C, D, E]             lastIncludedIndex=3
					^ new sentinel, keeps D and E
		*/
		rf.log = rf.log[rf.logIndex(args.LastIncludedIndex):]
	} else {
		// snapshot covers everything already, reset log to just sentinel
		/*
		Follower log: [S, A, B]    lastIncludedIndex=0
		Snapshot covers up to index 7 — follower doesn't even have index 7
		After trim:   [{Term: snapshotTerm, nil}]   just a sentinel
		*/
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// Update commit and applied indices
	// Everything up to LastIncludedIndex is already committed and applied in the snapshot.
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	// Persist and save snapshot
	rf.persistWithSnapshot(args.Data)

	// send snaphsot to state machine via applyCh
	/*
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
            Snapshot:      args.Data,
            SnapshotTerm:  args.LastIncludedTerm,
            SnapshotIndex: args.LastIncludedIndex,
		}
	}()
	*/

	// Do not send snapshot here, otherwise concurrency with applyloop
	// we note/archive the snapshot and let it to be sent in applyloop
	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}


// Send Snapshot to peer if peer log too far behind
func (rf *Raft) sendSnapshot(peer int) {
	// Call Snapshot RPC
	args := &InstallSnapshotArgs{
        Term:              rf.currentTerm,
        LeaderId:          rf.me,
        LastIncludedIndex: rf.lastIncludedIndex,
        LastIncludedTerm:  rf.lastIncludedTerm,
        Data:              rf.persister.ReadSnapshot(),
    }
    rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)

	// If fail then skip
	if !ok { return }

	// 
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Stale reply
	if rf.state != "leader" || rf.currentTerm != args.Term { return }

	// Demote if higher term seen
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
        rf.state = "follower"
        rf.votedFor = -1
        rf.persist()
        return
	}

	// Update nextIndex and matchIndex
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
}



// Snapshot Index Helpers

// get index in current log without snapshot
/*
Before snapshot:  log = [S, A, B, C, D]
                         0  1  2  3  4   ← array pos == absolute index, no problem

After snapshot at 2:  log = [B, C, D]
                              0  1  2   ← array pos 0 = absolute index 2
*/ 
func (rf *Raft) logIndex(absoluteIndex int) int {
	return absoluteIndex - rf.lastIncludedIndex
}

// get full length of the log
/*
lastIncludedIndex=2, log=[B,C,D] (len=3)
lastLogIndex = 2 + 3 - 1 = 4  ← correct absolute index of last entry
*/
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}


// get log term
func (rf *Raft) logTerm(absoluteIndex int) int {
	// Special case: if someone asks for the term of lastIncludedIndex itself
	// that entry no longer exists in the log — it was the last thing trimmed. 
	// But you still know its term because you saved it in lastIncludedTerm. 
	if absoluteIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	// look up normally
	return rf.log[rf.logIndex(absoluteIndex)].Term
}

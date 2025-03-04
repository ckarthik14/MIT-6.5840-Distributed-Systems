package raft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------
// From the first code: status, LogEntry, ApplyMsg, etc.
// ---------------------------------------------------------------

type status int

const (
	FOLLOWER status = iota
	CANDIDATE
	LEADER
)

const (
	ELECTION_TIMEOUT_MIN = 300
	ELECTION_TIMEOUT_MAX = 500
)

type LogEntry struct {
	Term  int
	Entry interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// For persisting Raft state
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// ---------------------------------------------------------------
// The Raft struct with merged approach: we keep first approach fields
// and add the second approach's channels + background loop logic.
// ---------------------------------------------------------------
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	applyCh chan ApplyMsg

	// from first approach
	state status

	// persistent
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// last seen leader for this raft
	lastSeenLeader int

	// Additional timers/tracking
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	votesReceived   int

	// channels from second approach
	winElectCh  chan bool
	stepDownCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
}

// ---------------------------------------------------------------
// GetState (same signature as first approach, with possible logs).
// ---------------------------------------------------------------
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := (rf.state == LEADER)
	return term, isLeader
}

func (rf *Raft) LastSeenLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.lastSeenLeader
}

// ---------------------------------------------------------------
// Persist & ReadPersist: from first approach
// ---------------------------------------------------------------
func (rf *Raft) persist() {
	// lock should already be held
	var buf bytes.Buffer
	ps := &PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	if err := labgob.NewEncoder(&buf).Encode(ps); err != nil {
		log.Fatalf("Persist: encode error: %v", err)
	}
	rf.persister.Save(buf.Bytes(), nil)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var ps PersistentState
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	if err := dec.Decode(&ps); err != nil {
		log.Printf("ReadPersist: decode error: %v", err)
		return
	}
	rf.currentTerm = ps.CurrentTerm
	rf.votedFor = ps.VotedFor
	rf.log = ps.Log

	DPrintf(rf.me, dTerm, "ReadPersist: Term=%d, VotedFor=%d, LogLen=%d",
		rf.currentTerm, rf.votedFor, len(rf.log))
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 3D: your code here

}

// ---------------------------------------------------------------
// Start(command): from first approach, but we rely on background
// broadcasting to replicate. Add logging calls back in.
// ---------------------------------------------------------------
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if !isLeader {
		return -1, term, false
	}

	index := len(rf.log)
	rf.log = append(rf.log, LogEntry{Term: term, Entry: command})

	DPrintf(rf.me, dLeader, "Received new command at index %d", index)
	DPrintf(rf.me, dLog, "Log after appending in master %v", rf.log)

	rf.persist()
	return index, term, true
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

// ---------------------------------------------------------------
// RequestVote RPC definitions from first approach, but with the logging
// from first approach reinserted as well.
// ---------------------------------------------------------------
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf(rf.me, dVote, "Received vote request from S%d for term %d", args.CandidateId, args.Term)

	if args.Term < rf.currentTerm {
		// lower term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf(rf.me, dVote, "Rejected vote for S%d - lower term (current: %d, received: %d)",
			args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf(rf.me, dTerm, "Updated term to %d due to vote request from S%d", args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Check if we already voted for someone else
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf(rf.me, dVote, "Rejected vote for S%d - already voted for S%d",
			args.CandidateId, rf.votedFor)
		return
	}

	// Check log up-to-date
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		DPrintf(rf.me, dVote, "Rejected vote for S%d - log not up to date (candidate: T%d/I%d, local: T%d/I%d)",
			args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		return
	}

	DPrintf(rf.me, dVote, "Granted vote to S%d for term %d", args.CandidateId, args.Term)
	rf.votedFor = args.CandidateId
	// signal that we granted vote
	rf.sendToChannel(rf.grantVoteCh, true)
	reply.VoteGranted = true
}

// We'll keep the same "sendRequestVote" signature from the first approach,
// reinsert the relevant logs from first approach. Also integrate the second approach's logic.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf(rf.me, dError, "Failed to contact S%d for RequestVote", server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// no longer candidate or term changed?
	if rf.state != CANDIDATE || args.Term != rf.currentTerm {
		return ok
	}

	// higher term => step down
	if reply.Term > rf.currentTerm {
		DPrintf(rf.me, dTerm, "Stepping down - higher term from S%d (%d > %d)",
			server, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.sendToChannel(rf.stepDownCh, true)
		return ok
	}

	if reply.VoteGranted && rf.state == CANDIDATE {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 {
			// majority
			rf.sendToChannel(rf.winElectCh, true)
		}
	}
	return ok
}

// ---------------------------------------------------------------
// AppendEntries from first approach, with logs reinserted from
// original code. We adopt the conflict logic from second approach
// but keep the original debug prints from the first code as best we can.
// ---------------------------------------------------------------
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if len(args.Entries) > 0 {
		DPrintf(rf.me, dLog, "Received %d entries from leader S%d", len(args.Entries), args.LeaderId)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		DPrintf(rf.me, dLog, "Rejected append from S%d - lower term", args.LeaderId)
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf(rf.me, dTerm, "Updated term to %d due to append from S%d", args.Term, args.LeaderId)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	// Signal we got heartbeat from valid leader
	rf.sendToChannel(rf.heartbeatCh, true)
	rf.lastSeenLeader = args.LeaderId

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	// If we don't have PrevLogIndex
	if rf.getLastLogIndex() < args.PrevLogIndex {
		DPrintf(rf.me, dLog, "Log too short - local: %d, required: %d",
			rf.getLastLogIndex(), args.PrevLogIndex)
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		return
	}

	// If mismatch in term
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		DPrintf(rf.me, dLog, "Term mismatch at index %d (local: %d, leader: %d)",
			args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

		reply.ConflictTerm = conflictTerm
		// find the first index for that conflictTerm
		idx := args.PrevLogIndex
		for idx >= 0 && rf.log[idx].Term == conflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		return
	}

	// they match => append new entries
	logIdx := args.PrevLogIndex + 1
	entriesIdx := 0
	for entriesIdx < len(args.Entries) && logIdx < len(rf.log) {
		if rf.log[logIdx].Term != args.Entries[entriesIdx].Term {
			// mismatch => truncate
			DPrintf(rf.me, dLog, "Truncated log at index %d due to term mismatch: %v", logIdx, rf.log)
			rf.log = rf.log[:logIdx]
			break
		}
		entriesIdx++
		logIdx++
	}

	if entriesIdx < len(args.Entries) {
		DPrintf(rf.me, dLog, "Appending %d new entries starting at index %d",
			len(args.Entries[entriesIdx:]), logIdx)
		rf.log = append(rf.log, args.Entries[entriesIdx:]...)
		DPrintf(rf.me, dLog, "Log after appending in follower %v", rf.log)
	}

	reply.Success = true

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		DPrintf(rf.me, dCommit, "Updated commit index to %d", rf.commitIndex)
		go rf.applyLogs()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf(rf.me, dError, "Failed to contact S%d for AppendEntries", server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// if not leader or term changed, ignore
	if rf.state != LEADER || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		DPrintf(rf.me, dTerm, "Stepping down - higher term from S%d (%d > %d)",
			server, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.sendToChannel(rf.stepDownCh, true)
		return ok
	}

	if reply.Success {
		matchIndex := args.PrevLogIndex + len(args.Entries)
		DPrintf(rf.me, dLeader, "S%d acknowledged entries up to index %d", server, matchIndex)

		if matchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = matchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		DPrintf(rf.me, dLeader, "Updated nextIndex for S%d to %d",
			server, rf.nextIndex[server])

	} else {
		// conflict
		if reply.ConflictTerm < 0 {
			// log is too short
			rf.nextIndex[server] = reply.ConflictIndex
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			// find conflictTerm
			newNext := rf.getLastLogIndex()
			for ; newNext >= 0; newNext-- {
				if rf.log[newNext].Term == reply.ConflictTerm {
					break
				}
			}
			if newNext < 0 {
				// not found
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = newNext
			}
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	}

	// update commit if majority
	for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term == rf.currentTerm {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf(rf.me, dCommit, "Committed index %d (majority reached)", n)
				go rf.applyLogs()
				break
			}
		}
	}
	return ok
}

// ---------------------------------------------------------------
// Helper to broadcast AppendEntries, with logs as you had them.
// ---------------------------------------------------------------
func (rf *Raft) broadcastAppendEntries() {
	if rf.state != LEADER {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		prevIndex := rf.nextIndex[i] - 1
		entries := []LogEntry{}
		if rf.nextIndex[i] <= rf.getLastLogIndex() {
			entries = rf.log[prevIndex+1:]
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  rf.log[prevIndex].Term,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}

		DPrintf(rf.me, dLeader, "Sending %d entries to S%d (prevIndex: %d, prevTerm: %d)",
			len(entries), i, prevIndex, args.PrevLogTerm)

		go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
	}
}

// ---------------------------------------------------------------
// applyLogs: same as second approach, but add the original debug prints
// you had for applying logs. We'll assume each command application is logged.
// ---------------------------------------------------------------
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		DPrintf(rf.me, dLog2, "Applying command at index %d", rf.lastApplied)

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Entry,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		st := rf.state
		rf.mu.Unlock()

		switch st {
		case LEADER:
			// keep sending heartbeats
			select {
			case <-rf.stepDownCh:
				// stepped down
			case <-time.After(120 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}

		case FOLLOWER:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.electionTimeoutDuration()):
				DPrintf(rf.me, dTimer, "Election timeout reached, starting election")
				rf.convertToCandidate(FOLLOWER)
			}

		case CANDIDATE:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(rf.electionTimeoutDuration()):
				DPrintf(rf.me, dTimer, "Election timeout reached, starting election")
				rf.convertToCandidate(CANDIDATE)
			}
		}
	}
}

// convertToCandidate
func (rf *Raft) convertToCandidate(from status) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != from {
		return
	}

	rf.resetChannels()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.persist()

	DPrintf(rf.me, dElect, "Starting election for term %d", rf.currentTerm)
	rf.broadcastRequestVote()
}

func (rf *Raft) broadcastRequestVote() {
	if rf.state != CANDIDATE {
		return
	}
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
		}(i)
	}
}

// convertToLeader
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CANDIDATE {
		return
	}
	rf.resetChannels()
	rf.state = LEADER

	lastIndex := rf.getLastLogIndex() + 1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}

	DPrintf(rf.me, dElect, "Server %d became leader with Term %d", rf.me, rf.currentTerm)
	// send initial heartbeats
	rf.broadcastAppendEntries()
}

// Utility for non-blocking channel-sends
func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

// reset channels
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

// election timeout with randomization
func (rf *Raft) electionTimeoutDuration() time.Duration {
	ms := ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// A simple min function for convenience
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------
// Make(...) merges the first approach's creation with the second approach's
// background goroutines (runServer).
// ---------------------------------------------------------------
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1) // index 0 is dummy
	rf.log[0] = LogEntry{Term: 0, Entry: nil}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.votesReceived = 0
	rf.electionTimeout = rf.electionTimeoutDuration()

	rf.lastSeenLeader = 0

	rf.resetChannels()

	// initialize from persisted state
	rf.readPersist(persister.ReadRaftState())

	// start server loop
	go rf.runServer()

	// optionally also start a separate apply loop if you prefer
	// (though in this code, we do go rf.applyLogs() after commits.)
	return rf
}

package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

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

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32
	applyCh   chan ApplyMsg
	state     status

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimeout  time.Duration
	lastHeartbeat    time.Time
	lastHeard        time.Time
	votesReceived    int
	heartbeatTimeout time.Duration
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == LEADER
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (3C).
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf(rf.me, dVote, "Received vote request from S%d for term %d", args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf(rf.me, dVote, "Rejected vote for S%d - lower term (current: %d, received: %d)",
			args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		DPrintf(rf.me, dTerm, "Updated term to %d due to vote request from S%d", args.Term, args.CandidateId)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	if rf.votedFor != -1 {
		reply.VoteGranted = false
		DPrintf(rf.me, dVote, "Rejected vote for S%d - already voted for S%d",
			args.CandidateId, rf.votedFor)
		return
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		DPrintf(rf.me, dVote, "Rejected vote for S%d - log not up to date (candidate: T%d/I%d, local: T%d/I%d)",
			args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		return
	}

	DPrintf(rf.me, dVote, "Granted vote to S%d for term %d", args.CandidateId, args.Term)
	rf.votedFor = args.CandidateId
	rf.electionTimeout = ElectionTimeout()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		DPrintf(rf.me, dLog, "Received %d entries from leader S%d", len(args.Entries), args.LeaderId)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf(rf.me, dLog, "Rejected append from S%d - lower term", args.LeaderId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		DPrintf(rf.me, dTerm, "Updated term to %d due to append from S%d", args.Term, args.LeaderId)
	}

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	if len(args.Entries) == 0 {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
			DPrintf(rf.me, dCommit, "Updated commit index to %d from heartbeat", rf.commitIndex)
		}
		reply.Success = true
		return
	}

	if rf.getLastLogIndex() < args.PrevLogIndex {
		DPrintf(rf.me, dLog, "Log too short - local: %d, required: %d",
			rf.getLastLogIndex(), args.PrevLogIndex)
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf(rf.me, dLog, "Term mismatch at index %d (local: %d, leader: %d)",
			args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		xIndex := args.PrevLogIndex

		for {
			if rf.log[xIndex].Term != reply.XTerm {
				break
			}
			xIndex--
		}
		reply.XIndex = xIndex + 1
		return
	}

	logIdx := args.PrevLogIndex + 1
	entriesIdx := 0
	for entriesIdx < len(args.Entries) && logIdx < len(rf.log) {
		if args.Entries[entriesIdx].Term != rf.log[logIdx].Term {
			rf.log = rf.log[:logIdx]
			DPrintf(rf.me, dLog, "Truncated log at index %d due to term mismatch", logIdx)
			break
		}
		entriesIdx++
		logIdx++
	}

	if entriesIdx < len(args.Entries) {
		DPrintf(rf.me, dLog, "Appending %d new entries starting at index %d",
			len(args.Entries[entriesIdx:]), logIdx)
		rf.log = append(rf.log, args.Entries[entriesIdx:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		DPrintf(rf.me, dCommit, "Updated commit index to %d", rf.commitIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if !isLeader {
		rf.mu.Unlock()
		return -1, term, isLeader
	}

	index := len(rf.log)
	rf.log = append(rf.log, LogEntry{Term: term, Entry: command})
	DPrintf(rf.me, dLeader, "Received new command at index %d", index)

	acksReceived := 1
	leaderCommit := rf.commitIndex
	lastLogIndex := rf.getLastLogIndex()
	nextIndex := make([]int, len(rf.nextIndex))
	copy(nextIndex, rf.nextIndex)

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				for {
					prevLogIndex := nextIndex[peer] - 1
					prevLogTerm := rf.log[nextIndex[peer]-1].Term
					entries := make([]LogEntry, lastLogIndex-prevLogIndex)
					copy(entries, rf.log[prevLogIndex+1:])

					DPrintf(rf.me, dLeader, "Sending %d entries to S%d (prevIndex: %d, prevTerm: %d)",
						len(entries), peer, prevLogIndex, prevLogTerm)

					args := &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: leaderCommit,
					}

					reply := &AppendEntriesReply{}

					if rf.sendAppendEntries(peer, args, reply) {
						rf.mu.Lock()

						if rf.state != LEADER {
							rf.mu.Unlock()
							break
						}

						if reply.Success {
							DPrintf(rf.me, dLeader, "S%d acknowledged entries up to index %d",
								peer, index)
							acksReceived++
							rf.nextIndex[peer] = max(rf.nextIndex[peer], index+1)
							rf.matchIndex[peer] = max(rf.matchIndex[peer], index)

							if acksReceived > len(rf.peers)/2 {
								rf.commitIndex = max(rf.commitIndex, index)
								DPrintf(rf.me, dCommit, "Committed index %d (majority reached)", index)
							}
							rf.mu.Unlock()
							break
						} else {
							if term >= reply.Term {
								if reply.XTerm == -1 {
									nextIndex[peer] = reply.XLen
								} else {
									nextIndex[peer] = reply.XIndex
								}
								DPrintf(rf.me, dLeader, "Updated nextIndex for S%d to %d",
									peer, nextIndex[peer])
								rf.mu.Unlock()
							} else {
								DPrintf(rf.me, dTerm, "Stepping down - higher term from S%d (%d > %d)",
									peer, reply.Term, term)
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.state = FOLLOWER
								rf.lastHeartbeat = time.Now()
								rf.mu.Unlock()
								break
							}
						}
					} else {
						DPrintf(rf.me, dError, "Failed to contact S%d", peer)
					}
				}
			}(peer)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func ElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

func HeartbeatInterval() time.Duration {
	return 150 * time.Millisecond
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.sendHeartbeats()
			continue
		}

		if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			DPrintf(rf.me, dTimer, "Election timeout reached, starting election")
			rf.startElection()
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) stateApplier() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied < rf.commitIndex {
			DPrintf(rf.me, dLog2, "Applying command at index %d", rf.lastApplied+1)

			rf.applyCh <- ApplyMsg{
				Command:      rf.log[rf.lastApplied+1].Entry,
				CommandIndex: rf.lastApplied + 1,
				CommandValid: true,
			}
			rf.lastApplied++
			DPrintf(rf.me, dLog2, "Applied command, lastApplied now %d", rf.lastApplied)
		}

		rf.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (rf *Raft) startElection() {
	DPrintf(rf.me, dElect, "Starting election for term %d", rf.currentTerm+1)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.electionTimeout = ElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.votesReceived = 1

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = FOLLOWER
						rf.lastHeartbeat = time.Now()
						return
					}
					// in case candidate has lost candidacy while looking for votes
					if rf.state != CANDIDATE {
						return
					}
					if reply.VoteGranted {
						rf.votesReceived++
						if rf.votesReceived > len(rf.peers)/2 {
							DPrintf(rf.me, dElect, "Server %d became leader with Term %d\n", rf.me, rf.currentTerm)
							rf.state = LEADER
							rf.lastHeartbeat = time.Now()

							for i := range rf.peers {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}

							rf.sendHeartbeats()
						}
					}
				}
			}(peer)
		}
	}
}

// sendHeartbeats sends AppendEntries RPCs to all peers to establish leadership.
func (rf *Raft) sendHeartbeats() {
	if rf.state != LEADER {
		return
	}

	//DPrintf("Server %d started sending heartbeats\n", rf.me)

	for peer := range rf.peers {
		if peer != rf.me {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: min(rf.matchIndex[peer], rf.commitIndex),
			}

			go func(peer int) {
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.lastHeartbeat = time.Now()
					}
				}
			}(peer)
		}
	}
	time.Sleep(HeartbeatInterval())
}

// Make creates a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.state = FOLLOWER

	// Initialize state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: 0, Entry: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	rf.electionTimeout = ElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.votesReceived = 0

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine to start elections
	go rf.ticker()

	// apply committed state
	go rf.stateApplier()

	return rf
}

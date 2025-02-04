package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log Entry.
//
// In part 3D you'll want to send other kinds of messages (e.g.,
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

// Constants for server states
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // Channel to send apply messages
	state     status              // Server state (Follower, Candidate, Leader)

	// persistent
	currentTerm int // Latest Term server has seen
	votedFor    int // CandidateID that received vote in current Term
	log         []LogEntry

	// volatile
	commitIndex int
	lastApplied int

	// volatile leader
	nextIndex  []int
	matchIndex []int

	// Election timer
	electionTimeout  time.Duration
	lastHeartbeat    time.Time
	lastHeard        time.Time
	votesReceived    int
	heartbeatTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == LEADER
	return term, isleader
}

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// See paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (3C).
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
}

// The service says it has created a snapshot that has
// all info up to and including index. This means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs defines the arguments structure for RequestVote RPC.
type RequestVoteArgs struct {
	Term         int // Candidate's Term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log Entry
	LastLogTerm  int // Term of candidate's last log Entry
}

// RequestVoteReply defines the reply structure for RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// AppendEntriesArgs defines the arguments structure for AppendEntries RPC.
type AppendEntriesArgs struct {
	Term     int // Leader's Term
	LeaderId int // Leader's ID

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply defines the reply structure for AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower accepts the append
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Got vote request at server %d from %d\n", rf.me, args.CandidateId)
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Enter request vote lock at server %d from %d\n", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Candidate has lower Term %d at server %d\n", args.CandidateId, rf.me)
		return
	}
	// follow the second rule in "Rules for Servers" in figure 2 before handling an incoming RPC
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		DPrintf("Candidate has higher Term %d at server %d\n", args.CandidateId, rf.me)
		//rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	// deny vote if already voted
	if rf.votedFor != -1 {
		reply.VoteGranted = false
		return
	}
	// deny vote if consistency check fails (candidate is less up-to-date)
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	DPrintf("Candidate last log term: %d, candidate last log index: %d\n", args.LastLogTerm, args.LastLogIndex)
	DPrintf("last log term: %d, last log index: %d\n", lastLogTerm, lastLogIndex)
	// election restriction
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		return
	}
	DPrintf("Server %d granted vote to candidate %d\n", rf.me, args.CandidateId)
	// now this peer must vote for the candidate
	rf.votedFor = args.CandidateId
	rf.electionTimeout = ElectionTimeout()
}

// SendRequestVote sends a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC handler (used for heartbeats).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("Append entries request at server %d from %d\n", rf.me, args.LeaderId)

	// outdated leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if Term is greater, update current Term and set to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	}

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	// if heartbeat
	if len(args.Entries) == 0 {
		// update leader commit
		rf.commitIndex = args.LeaderCommit
		reply.Success = true
		return
	}

	// no matching Term for previous log index
	if rf.getLastLogIndex() < args.PrevLogIndex {
		DPrintf("No matching term at server %d\n", rf.me)
		DPrintf("last log index: %d, args prev log index: %d\n", rf.getLastLogIndex(), args.PrevLogIndex)
		reply.Success = false
		return
	}

	// mismatch in Term at log
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Mismatch term %d != %d, prevLogIndex: %d at server %d\n", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, args.PrevLogIndex, rf.me)
		rf.log = rf.log[:args.PrevLogIndex+1]
		reply.Success = false
		return
	}

	DPrintf("Appending log entries %v at server %d from %d\n", args.Entries, rf.me, args.LeaderId)

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
}

// SendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start begins the agreement on the next command to be appended to Raft's log.
// If this server isn't the leader, returns false.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()

	term = rf.currentTerm
	isLeader = rf.state == LEADER

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// For 3B: Append command to log, update index
	index = len(rf.log)
	rf.log = append(rf.log, LogEntry{Term: term, Entry: command})

	// create a local copy of state for a particular command
	acksReceived := 1
	leaderId := rf.me

	log := make([]LogEntry, len(rf.log))
	copy(log, rf.log)
	leaderCommit := rf.commitIndex

	lastLogIndex := rf.getLastLogIndex()
	nextIndex := make([]int, len(rf.nextIndex))
	copy(nextIndex, rf.nextIndex)
	// end of local copy code

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				for {
					entries := make([]LogEntry, lastLogIndex-nextIndex[peer]+1)
					copy(entries, log[nextIndex[peer]:])

					prevLogIndex := nextIndex[peer] - 1
					prevLogTerm := log[nextIndex[peer]-1].Term

					DPrintf("Leader %d prepared Entries %v for server %d with prevlogindex %d and prevlogterm %d", leaderId, entries, peer, prevLogIndex, prevLogTerm)

					args := &AppendEntriesArgs{
						Term:         term,
						LeaderId:     leaderId,
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
							acksReceived++
							rf.nextIndex[peer] = index + 1
							rf.matchIndex[peer] = index

							if acksReceived > len(rf.peers)/2 {
								rf.commitIndex = index
							}
							rf.mu.Unlock()
							break
						} else {
							if term >= reply.Term {
								nextIndex[peer]--
								rf.mu.Unlock()
							} else {
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.state = FOLLOWER
								rf.lastHeartbeat = time.Now()
								rf.mu.Unlock()
								break
							}
						}
					} else {
						DPrintf("Leader %d could not contact server %d\n", rf.me, peer)
					}
				}
			}(peer)
		}
	}

	return index, term, isLeader
}

// Kill is called to stop the Raft instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// killed checks if this Raft instance has been killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ElectionTimeout generates a random duration between min and max milliseconds.
func ElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

// HeartbeatInterval returns the heartbeat interval duration.
func HeartbeatInterval() time.Duration {
	return 100 * time.Millisecond
}

// getLastLogIndex returns the index of the last log Entry.
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// getLastLogTerm returns the Term of the last log Entry.
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// Ticker is a goroutine that starts leader election periodically.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.sendHeartbeats()
			continue
		}

		// not a leader
		if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) stateApplier() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied < min(rf.commitIndex, rf.getLastLogIndex()) {
			DPrintf("Log while Applying state at server: %d, %v", rf.me, rf.log)
			DPrintf("lastApplied: %d, commitIndex: %d", rf.lastApplied, rf.commitIndex)
			DPrintf("Applying state at server: %d, command: %v, command index: %d", rf.me, rf.log[rf.lastApplied+1].Entry, rf.lastApplied+1)

			rf.applyCh <- ApplyMsg{
				Command:      rf.log[rf.lastApplied+1].Entry,
				CommandIndex: rf.lastApplied + 1,
				CommandValid: true,
			}
			rf.lastApplied++
		}

		rf.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

// startElection initiates a leader election.
func (rf *Raft) startElection() {
	DPrintf("Server %d started election with Term %d\n", rf.me, rf.currentTerm+1)
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
							DPrintf("Server %d became leader with Term %d\n", rf.me, rf.currentTerm)
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

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	for peer := range rf.peers {
		if peer != rf.me {
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

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
	"raftkv/labgob"
	"raftkv/labrpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	UseSnapshot bool
	Snapshot    []byte
}

type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

const (
	none = 0

	logPrefix = "[server-%d (term: %d, state: %s)]: "
)

// Raft is the consensus module.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// state StateType
	state atomic.Value

	// votes is the count of granted vote in the election
	votes int

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         *raftLog

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Channels between raft peers.
	applyCh       chan ApplyMsg
	grantVoteCh   chan struct{}
	winElectionCh chan struct{}
	heartbeatCh   chan struct{}
}

func (rf *Raft) randomizeElectionTimeout() {
	rf.electionTimeout = rf.electionTimeout + time.Duration(rand.Intn(int(rf.electionTimeout)))
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state.Load().(StateType) == StateLeader
}

// GetRaftStateSize returns the size of encoding Raft's
// persistent states (currentTerm, votedFor, log).
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// Save Raft persistent state to stable storage, where
// it can later be retrieved after a crash and restart.
//
// Persist when Raft persistent states are changed.
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.hardState())
}

// initialize persistent state.
func (rf *Raft) initialize(data []byte) {
	if len(data) == 0 {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	dec.Decode(&rf.currentTerm)
	dec.Decode(&rf.votedFor)
	dec.Decode(&rf.log.entries)
}

// raftHardState returns the encoding Raft persistent
// states.
func (rf *Raft) hardState() []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log.entries)
	return buf.Bytes()
}

// CreateSnapshot takes a snapshot (lastIncludedIndex, lastIncludeTerm,
// kv.data, kv.ack) for KV server.
func (rf *Raft) CreateSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(logPrefix+"create snapshot at index: %d", rf.me, rf.currentTerm, stmap[rf.state.Load().(StateType)], index)
	head := rf.log.head()
	lastIndex := rf.log.lastLogIndex()
	if index <= head.Index || index > lastIndex {
		// cannot compact log since index is invalid
		return
	}

	rf.compact(index, rf.log.term(index))
	head = rf.log.head()
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(head.Index)
	enc.Encode(head.Term)
	snapshot = append(buf.Bytes(), snapshot...)

	rf.persister.SaveStateAndSnapshot(rf.hardState(), snapshot)
}

func (rf *Raft) recoverFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	dec.Decode(&lastIncludedIndex)
	dec.Decode(&lastIncludedTerm)

	rf.log.lastApplied = lastIncludedIndex
	rf.log.commitIndex = lastIncludedIndex
	rf.compact(lastIncludedIndex, lastIncludedTerm)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
	rf.applyCh <- msg
}

func (rf *Raft) becomeFollower(term int) {
	rf.state.Store(StateFollower)
	rf.currentTerm = term
	rf.votedFor = none
}

func (rf *Raft) becomeCandidate() {
	rf.state.Store(StateCandidate)
	rf.votes = 1
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	rf.state.Store(StateLeader)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.log.lastLogIndex()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastIndex + 1
		if i == rf.me {
			rf.matchIndex[i] = lastIndex
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state.Load().(StateType) != StateCandidate || rf.currentTerm != args.Term {
		return ok
	}

	if rf.currentTerm < reply.Term {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return ok
	}

	if reply.VoteGranted {
		rf.votes++
		if rf.votes > len(rf.peers)/2 {
			DPrintf(logPrefix+"leader is elected", rf.me, rf.currentTerm, stmap[rf.state.Load().(StateType)])
			rf.becomeLeader()
			rf.persist()
			rf.winElectionCh <- struct{}{}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state.Load().(StateType) != StateLeader || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	} else {
		rf.nextIndex[server] = min(reply.RejectHint, rf.log.lastLogIndex())
	}
	if rf.leaderCommit() {
		go rf.applyLog()
	}

	return ok
}

func (rf *Raft) leaderCommit() bool {
	match := make(sortedSlice, len(rf.peers))
	for peer := range rf.peers {
		match[peer] = rf.matchIndex[peer]
	}
	sort.Sort(match)
	n := match[(len(rf.peers)-1)/2]
	if n > rf.log.commitIndex {
		if rf.log.term(n) == rf.currentTerm {
			rf.log.commitIndex = n
			DPrintf(logPrefix+"new commit index: %d", rf.me, rf.currentTerm, stmap[rf.state.Load().(StateType)], n)
			go rf.broadcastHeartbeat()
			return true
		}
	}
	return false
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state.Load().(StateType) != StateLeader || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
}

// RequestVote is the RPC sent by StateCandidate.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if (rf.votedFor == none || rf.votedFor == args.CandidateId) &&
		rf.log.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.grantVoteCh <- struct{}{}
	}
	rf.persist()
}

// AppendEntries is the RPC sent by StateLeader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.heartbeatCh <- struct{}{}

	reply.Term = rf.currentTerm
	rf.persist()
	if args.PrevLogIndex > rf.log.lastLogIndex() {
		reply.RejectHint = rf.log.lastLogIndex() + 1
		return
	}

	head := rf.log.head()
	if args.PrevLogIndex >= head.Index {
		term := rf.log.term(args.PrevLogIndex)
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= head.Index; i-- {
				if rf.log.term(i) != term {
					reply.RejectHint = i + 1
					return
				}
			}
		} else {
			reply.Success = true
			rf.log.truncate(lower, args.PrevLogIndex-head.Index+1)
			rf.log.append(args.Entries...)
			rf.persist()
			if rf.log.commitIndex < args.LeaderCommit {
				rf.log.commitIndex = min(args.LeaderCommit, rf.log.lastLogIndex())
				go rf.applyLog()
			}
		}
	}
}

// InstallSnapshot is the RPC of creating a snapshot.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}

	rf.heartbeatCh <- struct{}{}

	reply.Term = rf.currentTerm
	if args.LastIncludedIndex > rf.log.commitIndex {
		rf.compact(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.log.lastApplied = args.LastIncludedIndex
		rf.log.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.hardState(), args.Data)

		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.applyCh <- msg
	}
}

// Apply log entries with index in range [lastApplied+1, commitIndex]
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ents := rf.log.nextEntries()
	for _, ent := range ents {
		msg := ApplyMsg{
			CommandIndex: ent.Index,
			CommandValid: true,
			Command:      ent.Command,
		}
		rf.applyCh <- msg
	}
	rf.log.lastApplied = rf.log.commitIndex
}

// discard old log entries up to lastIncludedIndex.
func (rf *Raft) compact(index, term int) {
	DPrintf(logPrefix+"doing compaction at index: %d, term: %d", rf.me, rf.currentTerm, stmap[rf.state.Load().(StateType)], index, term)
	l := newRaftLog(LogEntry{Index: index, Term: term})
	if i, found := rf.log.findSamePoint(index, term); found {
		l.append(rf.log.entrySet(i+1, upper)...)
	}
	rf.log = l
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastLogIndex(),
		LastLogTerm:  rf.log.lastLogTerm(),
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me && rf.state.Load().(StateType) == StateCandidate {
			go rf.sendRequestVote(peer, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	head := rf.log.head()
	snapshot := rf.persister.ReadSnapshot()
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me && rf.state.Load().(StateType) == StateLeader {
			rf.mu.Lock()
			if rf.nextIndex[peer] > head.Index {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  rf.log.term(rf.nextIndex[peer] - 1),
					Entries:      rf.log.entrySet(rf.nextIndex[peer]-head.Index, upper),
					LeaderCommit: rf.log.commitIndex,
				}
				rf.mu.Unlock()
				go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
			} else {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: head.Index,
					LastIncludedTerm:  head.Term,
					Data:              snapshot,
				}
				rf.mu.Unlock()
				go rf.sendInstallSnapshot(peer, args, &InstallSnapshotReply{})
			}
		}
	}
}

// Start propose a new log entry.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.state.Load().(StateType) == StateLeader

	if isLeader {
		index = rf.log.lastLogIndex() + 1
		term = rf.currentTerm
		proposal := LogEntry{index, term, command}
		rf.log.append(proposal)
		DPrintf(logPrefix+"propose log: %+v", rf.me, rf.currentTerm, stmap[rf.state.Load().(StateType)], proposal)

		rf.matchIndex[rf.me] = rf.log.lastLogIndex()
		rf.persist()
		go rf.broadcastHeartbeat()
	}
	return index, term, isLeader
}

func (rf *Raft) run() {
	for {
		switch rf.state.Load().(StateType) {
		case StateFollower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.electionTimeout * time.Millisecond):
				rf.state.Store(StateCandidate)
			}
		case StateCandidate:
			rf.mu.Lock()
			rf.becomeCandidate()
			rf.persist()
			rf.mu.Unlock()

			go rf.broadcastRequestVote()

			select {
			case <-rf.heartbeatCh:
				rf.state.Store(StateFollower)
			case <-rf.winElectionCh:
			case <-time.After(rf.electionTimeout * time.Millisecond):
			}
		case StateLeader:
			go rf.broadcastHeartbeat()
			time.Sleep(rf.heartbeatTimeout * time.Millisecond)
		}
	}
}

// Make creates a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.electionTimeout = time.Duration(400)
	rf.heartbeatTimeout = time.Duration(100)
	rf.randomizeElectionTimeout()

	rf.becomeFollower(none)
	rf.log = newRaftLog(LogEntry{Index: none, Term: none})

	rf.applyCh = applyCh
	rf.grantVoteCh = make(chan struct{}, 500)
	rf.winElectionCh = make(chan struct{}, 500)
	rf.heartbeatCh = make(chan struct{}, 500)

	// Initialization or crash recovery.
	rf.initialize(persister.ReadRaftState())
	rf.recoverFromSnapshot(persister.ReadSnapshot())

	go rf.run()

	return rf
}

func (rf *Raft) Kill() {
	// Kill the raft server process.
}

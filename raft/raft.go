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
	"sync"
	"time"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	UseSnapshot bool
	Snapshot    []byte
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Raft is the consensus module.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	role      Role
	voteCount int

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Channels between raft peers.
	applyCh       chan ApplyMsg
	grantVoteCh   chan struct{}
	winElectionCh chan struct{}
	heartbeatCh   chan struct{}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// GetRaftStateSize returns the size of encoding Raft's
// persistent states (currentTerm, votedFor, log).
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
// Persist when raft's persistent states are changed.
func (rf *Raft) persist() {
	data := rf.getPersistentStates()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	dec.Decode(&rf.currentTerm)
	dec.Decode(&rf.votedFor)
	dec.Decode(&rf.log)
}

// getPersistentStates returns the encoding Raft's persistent
// states.
func (rf *Raft) getPersistentStates() []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	return buf.Bytes()
}

// TakeSnapshot takes a snapshot (lastIncludedIndex, lastIncludeTerm,
// kv.data, kv.ack) for KV server.
func (rf *Raft) TakeSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	lastIndex := rf.getLastLogIndex()
	if index <= baseIndex || index > lastIndex {
		// can't trim log since index is invalid
		return
	}
	rf.trimLog(index, rf.log[index-baseIndex].Term)

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.log[0].Index)
	enc.Encode(rf.log[0].Term)
	snapshot = append(buf.Bytes(), snapshot...)

	rf.persister.SaveStateAndSnapshot(rf.getPersistentStates(), snapshot)
}

func (rf *Raft) recoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	dec.Decode(&lastIncludedIndex)
	dec.Decode(&lastIncludedTerm)

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.trimLog(lastIncludedIndex, lastIncludedTerm)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
	rf.applyCh <- msg
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.role != Candidate || rf.currentTerm != args.Term {
		return ok
	}

	if rf.currentTerm < reply.Term {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			// win the election!
			rf.role = Leader
			rf.persist()

			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			idx := rf.getLastLogIndex() + 1
			for i := range rf.nextIndex {
				rf.nextIndex[i] = idx
			}
			rf.winElectionCh <- struct{}{}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.role != Leader || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.getLastLogIndex())
	}

	baseIndex := rf.log[0].Index
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		// find if there exists an N to update commitIndex
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}
	return ok
}

// Apply log entries with index in range [lastApplied+1, commitIndex]
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandIndex: i,
			CommandValid: true,
			Command:      rf.log[i-baseIndex].Command,
		}
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.role != Leader || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
}

// discard old log entries up to lastIncludedIndex.
func (rf *Raft) trimLog(index int, term int) {
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: index, Term: term})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index && rf.log[i].Term == term {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me && rf.role == Candidate {
			go rf.sendRequestVote(peer, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	snapshot := rf.persister.ReadSnapshot()

	for peer := range rf.peers {
		if peer != rf.me && rf.role == Leader {
			if rf.nextIndex[peer] > baseIndex {
				args := new(AppendEntriesArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				}
				if rf.nextIndex[peer] <= rf.getLastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[peer]-baseIndex:]
				}
				args.LeaderCommit = rf.commitIndex

				go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
			} else {
				args := new(InstallSnapshotArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].Index
				args.LastIncludedTerm = rf.log[0].Term
				args.Data = snapshot

				go rf.sendInstallSnapshot(peer, args, &InstallSnapshotReply{})
			}
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == Leader

	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) run() {
	for {
		switch rf.role {
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+150)):
				rf.role = Candidate
				rf.persist()
			}
		case Candidate:
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.currentTerm++
			rf.persist()
			rf.mu.Unlock()

			go rf.broadcastRequestVote()

			select {
			case <-rf.heartbeatCh:
				rf.role = Follower
			case <-rf.winElectionCh:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+150)):
			}
		case Leader:
			go rf.broadcastHeartbeat()
			time.Sleep(50 * time.Millisecond)
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

	rf.role = Follower
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.grantVoteCh = make(chan struct{}, 1<<8)
	rf.winElectionCh = make(chan struct{}, 1<<8)
	rf.heartbeatCh = make(chan struct{}, 1<<8)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnapshot(persister.ReadSnapshot())
	rf.persist()

	go rf.run()

	return rf
}

func (rf *Raft) Kill() {
	// TODO: kill the raft server.
}

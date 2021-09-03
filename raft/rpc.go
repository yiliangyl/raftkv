package raft

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

// RequestVote is the RPC sent by CANDIDATE.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.grantVoteCh <- struct{}{}
	}
	rf.persist()
}

// Check if candidate's log is at least as new as the voter.
func (rf *Raft) isUpToDate(canTerm, canIndex int) bool {
	index, term := rf.getLastLogIndex(), rf.getLastLogTerm()
	return canTerm > term || (canTerm == term && canIndex >= index)
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
	Term         int
	Success      bool
	NextTryIndex int
}

// AppendEntries is the RPC sent by LEADER.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.heartbeatCh <- struct{}{}
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	baseIndex := rf.log[0].Index
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if rf.log[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if rf.commitIndex < args.LeaderCommit {
			// update commitIndex and apply log
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.heartbeatCh <- struct{}{}
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getPersistentStates(), args.Data)

		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.applyCh <- msg
	}
}

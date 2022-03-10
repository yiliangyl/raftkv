package raft

import "math"

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	lower = -1
	upper = math.MaxInt64
)

type raftLog struct {
	entries []LogEntry

	// Volatile state on all servers.
	lastApplied int
	commitIndex int
}

func newRaftLog(head LogEntry) *raftLog {
	return &raftLog{
		entries: []LogEntry{head},
	}
}

// slice index [lo, hi)
func (l *raftLog) entrySet(lo, hi int) []LogEntry {
	if lo > hi {
		return []LogEntry{}
	} else if lo == lower {
		return append([]LogEntry{}, l.entries[:hi]...)
	} else if hi == upper {
		return append([]LogEntry{}, l.entries[lo:]...)
	} else {
		return append([]LogEntry{}, l.entries[lo:hi]...)
	}
}

func (l *raftLog) nextEntries() []LogEntry {
	return l.entries[l.lastApplied-l.entries[0].Index+1 : l.commitIndex-l.entries[0].Index+1]
}

func (l *raftLog) append(ents ...LogEntry) {
	l.entries = append(l.entries, ents...)
}

// slice index [lo, hi)
func (l *raftLog) truncate(lo, hi int) {
	if lo == lower {
		l.entries = l.entries[:hi]
	} else if hi == upper {
		l.entries = l.entries[lo:]
	} else {
		l.entries = l.entries[lo:hi]
	}
}

func (l *raftLog) lastLogIndex() int {
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) lastLogTerm() int {
	return l.entries[len(l.entries)-1].Term
}

func (l *raftLog) term(index int) int {
	if index < l.entries[0].Index {
		panic("log is compacted")
	}
	return l.entries[index-l.entries[0].Index].Term
}

func (l *raftLog) head() LogEntry {
	return l.entries[0]
}

// Check if candidate's log is at least as up-to-date as the voter.
func (l *raftLog) isUpToDate(index, term int) bool {
	t, i := l.lastLogTerm(), l.lastLogIndex()
	return term > t || (term == t && index >= i)
}

func (l *raftLog) findSamePoint(index, term int) (int, bool) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Index == index && l.entries[i].Term == term {
			return i, true
		}
	}
	return lower, false
}

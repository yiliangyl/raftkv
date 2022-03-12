package raft

import "log"

const (
	// Debug Debugging mode
	Debug = true
)

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

type sortedSlice []int

func (s sortedSlice) Len() int           { return len(s) }
func (s sortedSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s sortedSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

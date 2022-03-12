package kv

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

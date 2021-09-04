package kv

import (
	"bytes"
	"raftkv/labgob"
	"raftkv/labrpc"
	"raftkv/raft"
	"sync"
	"time"
)

type Op struct {
	Command   string // "get"/"put"/"append"
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type Result struct {
	OK          bool
	ClientId    int64
	RequestId   int64
	WrongLeader bool
	Err         Err
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// key - value
	data map[string]string
	// client's latest request id (for deduplication).
	ack map[int64]int64
	// log index - result of applying entry
	resultChMap map[int]chan Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	entry := Op{
		Command:   "get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
	}

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{
		Command:   args.Command,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = result.Err
}

func (kv *KVServer) appendEntryToLog(entry Op) Result {
	idx, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.resultChMap[idx]; !ok {
		kv.resultChMap[idx] = make(chan Result, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultChMap[idx]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(250 * time.Millisecond): // TODO: What is a better time duration?
		return Result{OK: false}
	}
}

func isMatch(op Op, result Result) bool {
	return op.ClientId == result.ClientId && op.RequestId == result.RequestId
}

func (kv *KVServer) run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.UseSnapshot {
			buf := bytes.NewBuffer(msg.Snapshot)
			dec := labgob.NewDecoder(buf)

			var lastIncludedIndex, lastIncludedTerm int
			dec.Decode(&lastIncludedIndex)
			dec.Decode(&lastIncludedTerm)
			dec.Decode(&kv.data)
			dec.Decode(&kv.ack)
		} else {
			op := msg.Command.(Op)
			result := kv.applyOp(op)
			if ch, ok := kv.resultChMap[msg.CommandIndex]; ok {
				select {
				case <-ch: // drain bad data.
				default:
				}
			} else {
				kv.resultChMap[msg.CommandIndex] = make(chan Result, 1)
			}
			kv.resultChMap[msg.CommandIndex] <- result

			// create snapshot if Raft's state exceeds allowed size.
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				buf := new(bytes.Buffer)
				enc := labgob.NewEncoder(buf)
				enc.Encode(kv.data)
				enc.Encode(kv.ack)
				go kv.rf.TakeSnapshot(buf.Bytes(), msg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyOp(op Op) Result {
	result := Result{
		OK:          true,
		WrongLeader: false,
		ClientId:    op.ClientId,
		RequestId:   op.RequestId,
	}

	switch op.Command {
	case "put":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] = op.Value
		}
		result.Err = OK
	case "append":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] += op.Value
		}
		result.Err = OK
	case "get":
		if val, ok := kv.data[op.Key]; ok {
			result.Err = OK
			result.Value = val
		} else {
			result.Err = ErrNoKey
		}
	}
	kv.ack[op.ClientId] = op.RequestId
	return result
}

// Check if the request is duplicated with request id.
func (kv *KVServer) isDuplicated(op Op) bool {
	if lastRequestId, ok := kv.ack[op.ClientId]; ok {
		return lastRequestId >= op.RequestId
	}
	return false
}

// StartKVServer starts a KV server based on Raft.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	labgob.Register(Result{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 1<<8)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.resultChMap = make(map[int]chan Result)

	go kv.run()
	return kv
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
}

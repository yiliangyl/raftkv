package kv

import (
	"bytes"
	"raftkv/labgob"
	"raftkv/labrpc"
	"raftkv/raft"
	"sync"
	"time"
)

const (
	maxWaitRespTimeout = 250
)

const (
	logPrefix = "[kv-%d] "
)

type Proposal struct {
	Command   CommandType // get/put/append
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type Callback struct {
	reject    bool
	ClientId  int64
	RequestId int64
	ErrMsg    ErrMsg
	Value     string
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
	callbacks map[int]chan Callback
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	prop := Proposal{
		Command:   GetType,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
	}

	cb := kv.propose(prop)
	if cb.reject {
		reply.ErrMsg = ErrNotLeader
		return
	}

	reply.ErrMsg = cb.ErrMsg
	reply.Value = cb.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	prop := Proposal{
		Command:   args.Command,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}

	cb := kv.propose(prop)
	if cb.reject {
		reply.ErrMsg = ErrNotLeader
		return
	}

	reply.ErrMsg = cb.ErrMsg
}

func (kv *KVServer) propose(prop Proposal) Callback {
	idx, _, lead := kv.rf.Start(prop)
	if !lead {
		return Callback{reject: true}
	}

	kv.mu.Lock()
	if _, ok := kv.callbacks[idx]; !ok {
		kv.callbacks[idx] = make(chan Callback, 1)
	}
	cc := kv.callbacks[idx]
	kv.mu.Unlock()

	select {
	case cb := <-cc:
		if match(prop, cb) {
			return cb
		}
	case <-time.After(maxWaitRespTimeout * time.Millisecond): // TODO: What is a better time duration?
	}
	return Callback{reject: true}
}

func match(prop Proposal, cb Callback) bool {
	return prop.ClientId == cb.ClientId && prop.RequestId == cb.RequestId
}

func (kv *KVServer) run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.NeedSnapshot {
			buf := bytes.NewBuffer(msg.Snapshot)
			dec := labgob.NewDecoder(buf)

			var index, term int
			dec.Decode(&index)
			dec.Decode(&term)
			dec.Decode(&kv.data)
			dec.Decode(&kv.ack)
		} else {
			prop := msg.Command.(Proposal)
			cb := kv.apply(prop)
			if cc, ok := kv.callbacks[msg.CommandIndex]; ok {
				select {
				case <-cc: // drain bad data.
				default:
				}
			} else {
				kv.callbacks[msg.CommandIndex] = make(chan Callback, 1)
			}
			kv.callbacks[msg.CommandIndex] <- cb

			// create snapshot if Raft state exceeds allowed size.
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				buf := new(bytes.Buffer)
				enc := labgob.NewEncoder(buf)
				enc.Encode(kv.data)
				enc.Encode(kv.ack)
				go kv.rf.CreateSnapshot(buf.Bytes(), msg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) apply(prop Proposal) Callback {
	cb := Callback{
		ClientId:  prop.ClientId,
		RequestId: prop.RequestId,
	}

	switch prop.Command {
	case PutType:
		if !kv.stale(prop) {
			kv.data[prop.Key] = prop.Value
		}
		cb.ErrMsg = OK
	case AppendType:
		if !kv.stale(prop) {
			kv.data[prop.Key] += prop.Value
		}
		cb.ErrMsg = OK
	case GetType:
		if val, ok := kv.data[prop.Key]; ok {
			cb.ErrMsg = OK
			cb.Value = val
		} else {
			cb.ErrMsg = ErrNotFound
		}
	}
	kv.ack[prop.ClientId] = prop.RequestId
	DPrintf(logPrefix+"apply command type: %s, client id: %d, request id: %d", kv.me, prop.Command, prop.ClientId, prop.RequestId)
	return cb
}

// Check if the request is stale
func (kv *KVServer) stale(prop Proposal) bool {
	if last, ok := kv.ack[prop.ClientId]; ok {
		return prop.RequestId <= last
	}
	return false
}

// StartKVServer starts a KV server based on Raft.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Proposal{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 500)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.callbacks = make(map[int]chan Callback)

	go kv.run()
	return kv
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
}

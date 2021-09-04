package shardmaster

import (
	"raftkv/labgob"
	"raftkv/labrpc"
	"raftkv/raft"
	"sync"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	ack         map[int64]int64     // client's latest request id
	resultChMap map[int]chan Result // command index to notifying channel
}

type Op struct {
	Command   string
	ClientId  int64
	RequestId int64

	// for JoinArgs
	Servers map[int][]string // new GID -> servers mappings
	// for LeaveArgs
	GIDs []int
	// for MoveArgs
	Shard int
	GID   int
	// for QueryArgs
	Num int // desired config number
}

type Result struct {
	OK          bool
	ClientId    int64
	RequestId   int64
	WrongLeader bool
	Err         Err

	// for QueryReply
	Config Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

func (sm *ShardMaster) run() {

}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Result{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.ack = make(map[int64]int64)
	sm.resultChMap = make(map[int]chan Result)

	go sm.run()
	return sm
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
}

// Raft is needed by shardkv tester.
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

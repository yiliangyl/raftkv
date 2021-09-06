package shardmaster

import (
	"raftkv/labgob"
	"raftkv/labrpc"
	"raftkv/raft"
	"sync"
	"time"
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
	entry := Op{
		Command:   "join",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Servers:   args.Servers,
	}

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	entry := Op{
		Command:   "leave",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GIDs:      args.GIDs,
	}

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	entry := Op{
		Command:   "move",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	entry := Op{
		Command:   "query",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num:       args.Num,
	}

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Config = result.Config
}

func (sm *ShardMaster) appendEntryToLog(entry Op) Result {
	idx, _, isLeader := sm.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	sm.mu.Lock()
	if _, ok := sm.resultChMap[idx]; !ok {
		sm.resultChMap[idx] = make(chan Result, 1)
	}
	sm.mu.Unlock()

	select {
	case result := <-sm.resultChMap[idx]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(200 * time.Millisecond):
		return Result{OK: false}
	}
}

func isMatch(op Op, result Result) bool {
	return op.ClientId == result.ClientId && op.RequestId == result.RequestId
}

func (sm *ShardMaster) applyJoin(op Op) {
	config := sm.makeNextConfig()
	for gid, servers := range op.Servers {
		config.Groups[gid] = servers

		for i := 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}

	balanceShards(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) applyLeave(op Op) {
	config := sm.makeNextConfig()

	// find a replica group won't leave.
	stayGid := 0
	for gid := range config.Groups {
		stay := true
		for _, del := range op.GIDs {
			if gid == del {
				stay = false
				break
			}
		}
		if stay {
			stayGid = gid
			break
		}
	}

	for _, del := range op.GIDs {
		for i := 0; i < NShards; i++ {
			if config.Shards[i] == del {
				config.Shards[i] = stayGid
			}
		}
		delete(config.Groups, del)
	}

	balanceShards(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) applyMove(op Op) {
	config := sm.makeNextConfig()
	config.Shards[op.Shard] = op.GID
	// TODO: why don't need to balance shards?
	// balanceShards(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) makeNextConfig() Config {
	curr := sm.configs[len(sm.configs)-1]

	next := Config{}
	next.Num = curr.Num + 1
	next.Shards = curr.Shards
	next.Groups = make(map[int][]string)
	for gid, servers := range curr.Groups {
		next.Groups[gid] = servers
	}
	return next
}

// balance shards among replica groups.
func balanceShards(config *Config) {
	g2s := makeGidToShards(config)
	if len(config.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
	} else {
		mean := NShards / len(config.Groups)
		numToMove := 0
		for _, shards := range g2s {
			if len(shards) > mean {
				numToMove += len(shards) - mean
			}
		}
		for i := 0; i < numToMove; i++ {
			// each time move a shard from replica group with most shards to replica
			// group with fewest shards.
			src, dst := getGidPair(g2s)
			N := len(g2s[src]) - 1
			config.Shards[g2s[src][N]] = dst
			g2s[dst] = append(g2s[dst], g2s[src][N])
			g2s[src] = g2s[src][:N]
		}
	}
}

func makeGidToShards(config *Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

// Find a pair of gids (src, dst) to move one shard.
// src: the most shards
// dst: the fewest shards
func getGidPair(g2s map[int][]int) (src, dst int) {
	for gid, shards := range g2s {
		if src == 0 || len(shards) > len(g2s[src]) {
			src = gid
		}
		if dst == 0 || len(shards) < len(g2s[dst]) {
			dst = gid
		}
	}
	return
}

func (sm *ShardMaster) run() {
	for {
		msg := <-sm.applyCh
		sm.mu.Lock()
		op := msg.Command.(Op)
		result := sm.applyOp(op)
		if ch, ok := sm.resultChMap[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			sm.resultChMap[msg.CommandIndex] = make(chan Result, 1)
		}
		sm.resultChMap[msg.CommandIndex] <- result
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) applyOp(op Op) Result {
	result := Result{
		OK:          true,
		ClientId:    op.ClientId,
		RequestId:   op.RequestId,
		WrongLeader: false,
	}

	switch op.Command {
	case "join":
		if !sm.isDuplicated(op) {
			sm.applyJoin(op)
		}
		result.Err = OK
	case "leave":
		if !sm.isDuplicated(op) {
			sm.applyLeave(op)
		}
		result.Err = OK
	case "move":
		if !sm.isDuplicated(op) {
			sm.applyMove(op)
		}
		result.Err = OK
	case "query":
		if op.Num == -1 || op.Num >= len(sm.configs) {
			result.Config = sm.configs[len(sm.configs)-1]
		} else {
			result.Config = sm.configs[op.Num]
		}
		result.Err = OK
	}
	sm.ack[op.ClientId] = op.RequestId
	return result
}

func (sm *ShardMaster) isDuplicated(op Op) bool {
	if lastRequestId, ok := sm.ack[op.ClientId]; ok {
		return lastRequestId >= op.RequestId
	}
	return false
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
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

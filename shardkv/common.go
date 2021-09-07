package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// ShardMaster decides which group serves each shard.
// ShardMaster may change shard assignment from time to time.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "put" or "append"

	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string

	ClientId  int64
	RequestId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

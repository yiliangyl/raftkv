package kv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutAppendArgs struct {
	Key     string
	Value   string
	Command string // "Put" or "Append"

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

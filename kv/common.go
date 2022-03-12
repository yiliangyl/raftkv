package kv

const (
	OK           = "OK"
	ErrNotFound  = "ErrNotFound"
	ErrNotLeader = "ErrNotLeader"
)

type ErrMsg string

func (err ErrMsg) OK() bool {
	return err == OK
}

func (err ErrMsg) IsErrNotFound() bool {
	return err == ErrNotFound
}

func (err ErrMsg) IsErrNotLeader() bool {
	return err == ErrNotLeader
}

type CommandType string

const (
	GetType    CommandType = "get"
	PutType    CommandType = "put"
	AppendType CommandType = "append"
)

type PutAppendArgs struct {
	Key       string
	Value     string
	Command   CommandType
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	ErrMsg ErrMsg
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	ErrMsg ErrMsg
	Value  string
}

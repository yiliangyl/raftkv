package kv

import (
	"crypto/rand"
	"math/big"
	"raftkv/labrpc"
	"sync"
	"time"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd

	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

// Get fetches the current value for a key.
// returns "" if the key does not exist.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
	}
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply GetReply
			ok := srv.Call("KVServer.Get", &args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// PutAppend is shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
func (ck *Clerk) putAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Command:  op,
		ClientId: ck.clientId,
	}
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply PutAppendReply
			ok := srv.Call("KVServer.PutAppend", &args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, "put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, "append")
}

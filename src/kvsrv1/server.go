package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Requestinf struct {
	RequestId rpc.Id
	Err       rpc.Err
}

type KVServer struct {
	mu          sync.Mutex
	kv          map[string]ValueVersion
	lastRequest map[rpc.Id]rpc.Id
	// Your definitions here.
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kv = make(map[string]ValueVersion)
	kv.lastRequest = make(map[rpc.Id]rpc.Id)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.kv[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = v.Value
	reply.Version = v.Version
	reply.Err = rpc.OK
	// Your code here.
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(kv.lastRequest) > 100000 {
		kv.lastRequest = make(map[rpc.Id]rpc.Id)
	}

	if last, ok := kv.lastRequest[args.ClientId]; ok {
		if args.RequestId <= last {
			reply.Err = rpc.OK
			return
		}
	}

	// if last, ok := kv.lastRequest[args.ClientId]; ok {
	// 	if args.RequestId <= last.RequestId {
	// 		reply.Err = last.Err
	// 		return
	// 	}
	// }

	v, ok := kv.kv[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		kv.kv[args.Key] = ValueVersion{
			Value:   args.Value,
			Version: 1,
		}

		reply.Err = rpc.OK
	} else if args.Version != v.Version {
		reply.Err = rpc.ErrVersion
	} else {
		kv.kv[args.Key] = ValueVersion{
			Value:   args.Value,
			Version: args.Version + 1,
		}

		reply.Err = rpc.OK
	}

	// kv.lastRequest[args.ClientId] = Requestinf{
	// 	RequestId: args.RequestId,
	// 	Err:       reply.Err,
	// }
	if reply.Err == rpc.OK {
		kv.lastRequest[args.ClientId] = args.RequestId
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

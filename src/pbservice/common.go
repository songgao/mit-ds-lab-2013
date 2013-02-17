package pbservice

import "errors"

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrWrongViewnum = "ErrWrongViewnum"
)

var RPCERR = errors.New("RPC Error")

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type SyncArgs struct {
	Data    map[string]string
	Viewnum uint
}

type SyncReply struct {
	Err Err
}

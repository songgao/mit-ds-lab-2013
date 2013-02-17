package pbservice

import "viewservice"
import "net/rpc"
import "time"
import "sync"

// You'll probably need to uncomment this:
// import "time"

type Clerk struct {
	vs   *viewservice.Clerk
	view viewservice.View
	mu   sync.Mutex
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	ok := false
	for !ok {
		ck.view, ok = ck.vs.Get()
		time.Sleep(viewservice.PingInterval)
	}
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.

	if key == "" {
		return ErrNoKey
	}

	ck.mu.Lock()
	defer ck.mu.Unlock()

	reply := new(GetReply)
	ok := call(ck.view.Primary, "PBServer.Get", &GetArgs{Key: key}, reply)
	for !(ok && reply.Err == OK) {
		ck.view, ok = ck.vs.Get()
		if ok {
			ok = call(ck.view.Primary, "PBServer.Get", &GetArgs{Key: key}, reply)
		}
		time.Sleep(viewservice.PingInterval)
	}

	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {

	// Your code here.

	if key == "" {
		return
	}

	ck.mu.Lock()
	defer ck.mu.Unlock()

	reply := new(PutReply)
	ok := call(ck.view.Primary, "PBServer.Put", &PutArgs{Key: key, Value: value}, reply)
	for !(ok && reply.Err == OK) {
		ck.view, ok = ck.vs.Get()
		if ok {
			ok = call(ck.view.Primary, "PBServer.Put", &PutArgs{Key: key, Value: value}, reply)
		}
		time.Sleep(viewservice.PingInterval)
	}
}

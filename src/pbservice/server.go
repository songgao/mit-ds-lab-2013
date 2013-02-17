package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view      viewservice.View
	data      map[string]string
	pingCount int
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary == pb.me {
		str, ok := pb.data[args.Key]
		if ok {
			reply.Value = str
		} else {
			reply.Value = ""
		}
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else if pb.view.Backup != "" {
		ok := call(pb.view.Backup, "PBServer.SyncPut", args, reply)
		if !ok {
			return RPCERR
		}
		if reply.Err != OK {
			return nil
		}
	}

	pb.data[args.Key] = args.Value
	reply.Err = OK

	return nil
}

func (pb *PBServer) SyncPut(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup == pb.me {
		pb.data[args.Key] = args.Value
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) SyncAll(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum == pb.view.Viewnum {
		pb.data = args.Data
		reply.Err = OK
	} else {
		reply.Err = ErrWrongViewnum
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.pingCount > viewservice.DeadPings {
		pb.view.Viewnum = 0
		pb.view.Primary = ""
		pb.view.Backup = ""
		pb.pingCount = 0
	} else {
		view, err := pb.vs.Ping(pb.view.Viewnum)
		if err == nil {
			pb.pingCount = 0
			if view.Primary == pb.me && view.Backup != "" && view.Backup != pb.view.Backup {
				// need to Sync
				reply := new(SyncReply)
				ok := call(view.Backup, "PBServer.SyncAll", &SyncArgs{Data: pb.data, Viewnum: view.Viewnum}, reply)
				if ok && reply.Err == OK {
					pb.view = view
				}
			} else {
				pb.view = view
			}
		} else {
			pb.pingCount++
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	var err error
	pb.view, err = pb.vs.Ping(0)
	for err != nil {
		pb.view, err = pb.vs.Ping(0)
		time.Sleep(viewservice.PingInterval)
	}
	pb.data = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}

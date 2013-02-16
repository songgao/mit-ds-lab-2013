package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "errors"

// errors
var (
	NOT_IMPLEMENTED = errors.New("Not implemented!")
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.

	servers     map[string]time.Time
	currentView View
	acked       bool
}

func (vs *ViewServer) newViewNum() {
	if vs.currentView.Viewnum == ^uint(0) {
		vs.currentView.Viewnum = 0
	} else {
		vs.currentView.Viewnum++
	}
}

func (vs *ViewServer) changeView(p string, b string) bool {
	if vs.acked && (vs.currentView.Primary != p || vs.currentView.Backup != b) {
		vs.currentView.Primary = p
		vs.currentView.Backup = b
		vs.newViewNum()
		vs.acked = false
		return true
	}
	return false
}

func (vs *ViewServer) getNewBackup() string {
	for k := range vs.servers {
		if k != vs.currentView.Primary && k != vs.currentView.Backup {
			return k
		}
	}
	return ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.

	vs.servers[args.Me] = time.Now()

	if args.Viewnum == 0 {
		if "" == vs.currentView.Primary && "" == vs.currentView.Backup {
			vs.changeView(args.Me, "")
			reply.View = vs.currentView
			return nil
		} else {
			if vs.currentView.Primary == args.Me {
				// Primary restarted. Proceeding to a new view
				vs.changeView(vs.currentView.Backup, vs.getNewBackup())
			} else if vs.currentView.Backup == args.Me {
				// Backup restarted. Proceeding to a new view
				vs.changeView(vs.currentView.Primary, vs.getNewBackup())
			}
			reply.View = vs.currentView
			return nil
		}
	}

	if !vs.acked {
		if args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum {
			// the proceeding view is acknowledged
			vs.acked = true
		}
	}

	reply.View = vs.currentView
	return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currentView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.acked {
		for k, v := range vs.servers {
			if time.Since(v) > DeadPings*PingInterval {
				delete(vs.servers, k)
				if k == vs.currentView.Primary {
					vs.changeView(vs.currentView.Backup, vs.getNewBackup())
				} else if k == vs.currentView.Backup {
					vs.changeView(vs.currentView.Primary, vs.getNewBackup())
				}
			}
		}
		if vs.currentView.Backup == "" {
			vs.changeView(vs.currentView.Primary, vs.getNewBackup())
		}
		if vs.currentView.Primary == "" {
			vs.changeView(vs.currentView.Backup, vs.getNewBackup())
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.acked = true
	vs.servers = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}

package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

import "time"

type instance struct {
	MuProposor sync.Mutex
	N          int
	V          interface{}

	MuAcceptor sync.Mutex
	N_p        int         // highest prepare seen
	N_a        int         // highest accept seen
	V_a        interface{} // highest accept seen

	MuLearner sync.Mutex
	Decided   bool
	Value     interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*instance
	max_seq   int
	done      int
	dones     map[int]int // peer index --> Done number
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
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(name, args, reply)
	if err == nil {
		return true
	}
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	min_done := px.Min()

	px.mu.Lock()
	if seq < min_done { // ignore forgotten seq range
		px.mu.Unlock()
		return
	}

	ins, ok := px.instances[seq]
	if !ok {
		px.instances[seq] = new(instance)
		ins = px.instances[seq]
	}

	if seq > px.max_seq {
		px.max_seq = seq
	}
	px.mu.Unlock()

	ins.MuLearner.Lock()
	decided := ins.Decided
	ins.MuLearner.Unlock()
	if decided {
		return
	}

	ins.MuProposor.Lock()
	ins.V = v
	ins.MuProposor.Unlock()

	go px.proposer(seq)
}

func (px *Paxos) proposer(seq int) {
	px.mu.Lock()
	ins, ok := px.instances[seq]
	done := px.done
	me := px.me
	px.mu.Unlock()

	if !ok {
		return
	}

	ins.MuAcceptor.Lock()
	max_seen := ins.N_p
	ins.MuAcceptor.Unlock()

	ins.MuProposor.Lock()
	defer ins.MuProposor.Unlock()

	if ins.N == 0 {
		ins.N = px.me + 1
	} else {
		ins.N = (max_seen/len(px.peers)+1)*len(px.peers) + px.me + 1
	}

	// send prepare
	var preReq PrepareReq
	var preRsp PrepareRsp
	preReq.Seq = seq
	preReq.N = ins.N
	preReq.Done = done
	preReq.Me = me
	counter := 0
	connected := 0
	v_ := ins.V
	max_n := -1
	for i := range px.peers {
		ok := call(px.peers[i], "Paxos.AcceptorPrepare", &preReq, &preRsp)
		if ok {
			connected++
			if preRsp.OK {
				counter++
				if preRsp.N_a > max_n {
					max_n = preRsp.N_a
					v_ = preRsp.V_a
				}
			}
		}
	}

	if counter <= len(px.peers)/2 {
		if connected <= len(px.peers)/2 {
			// Is in minority; should slow down requests to save bandwidth
			time.Sleep(5 * time.Millisecond)
		}
		go px.proposer(seq)
		return
	}

	// send accept
	var actReq AcceptReq
	var actRsp AcceptRsp
	actReq.Seq = seq
	actReq.N = ins.N
	actReq.V = v_
	counter = 0
	for i := range px.peers {
		ok := call(px.peers[i], "Paxos.AcceptorAccept", &actReq, &actRsp)
		if ok {
			if actRsp.OK {
				counter++
			}
		}
	}

	if counter <= len(px.peers)/2 {
		go px.proposer(seq)
		return
	}

	// send decided
	var decReq DecidedReq
	var decRsp DecidedRsp
	decReq.Seq = seq
	decReq.V = v_
	for i := range px.peers {
		call(px.peers[i], "Paxos.LearnerDecided", &decReq, &decRsp)
	}
	px.LearnerDecided(&decReq, &decRsp)
}

func (px *Paxos) AcceptorPrepare(req *PrepareReq, rsp *PrepareRsp) error {
	px.mu.Lock()
	ins, ok := px.instances[req.Seq]
	if !ok {
		px.instances[req.Seq] = new(instance)
		ins = px.instances[req.Seq]
	}

	px.dones[req.Me] = req.Done
	px.mu.Unlock()

	ins.MuAcceptor.Lock()
	defer ins.MuAcceptor.Unlock()

	if req.N > ins.N_p {
		ins.N_p = req.N
		rsp.OK = true
		rsp.N_a = ins.N_a
		rsp.V_a = ins.V_a
	} else {
		rsp.OK = false
	}

	return nil
}

func (px *Paxos) AcceptorAccept(req *AcceptReq, rsp *AcceptRsp) error {
	px.mu.Lock()
	ins, ok := px.instances[req.Seq]
	if !ok {
		px.instances[req.Seq] = new(instance)
		ins = px.instances[req.Seq]
	}
	px.mu.Unlock()

	ins.MuAcceptor.Lock()
	defer ins.MuAcceptor.Unlock()

	if req.N >= ins.N_p {
		ins.N_p = req.N
		ins.N_a = req.N
		ins.V_a = req.V
		rsp.OK = true
	} else {
		rsp.OK = false
	}

	return nil
}

func (px *Paxos) LearnerDecided(req *DecidedReq, rsp *DecidedRsp) error {
	px.mu.Lock()
	ins, ok := px.instances[req.Seq]
	if !ok {
		px.instances[req.Seq] = new(instance)
		ins = px.instances[req.Seq]
	}
	px.mu.Unlock()

	ins.MuLearner.Lock()
	defer ins.MuLearner.Unlock()

	ins.Decided = true
	ins.Value = req.V

	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.done < seq {
		px.done = seq
		for k := range px.instances {
			if k <= seq {
				delete(px.instances, k)
			}
		}
	}

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.max_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min doesn't reflect another Peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() can't increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s won't increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	min_done := int(^uint(0) >> 1) // max value of int
	for k := range px.dones {
		if min_done > px.dones[k] {
			min_done = px.dones[k]
		}
	}
	return min_done + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer's state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	px.mu.Lock()
	ins, ok := px.instances[seq]
	px.mu.Unlock()
	if !ok {
		return false, nil
	}

	ins.MuLearner.Lock()
	defer ins.MuLearner.Unlock()
	return ins.Decided, ins.Value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this server's port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*instance)
	px.max_seq = -1
	px.done = -1
	px.dones = make(map[int]int)
	for k := range px.peers {
		px.dones[k] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
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
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
					px.Kill()
				}
			}
		}()
	}

	return px
}

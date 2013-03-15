package paxos

type PrepareReq struct {
	Seq  int
	N    int
	Done int
	Me   int
}

type PrepareRsp struct {
	OK      bool
	N_a     int
	V_a     interface{}
	Decided bool
}

type AcceptReq struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptRsp struct {
	OK bool
}

type DecidedReq struct {
	Seq int
	V   interface{}
}

type DecidedRsp struct {
}

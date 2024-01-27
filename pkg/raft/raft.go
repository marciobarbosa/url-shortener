package raft

import (
    "os"
    "fmt"
    "net"
    "sync"
    "time"
    "net/rpc"
    "math/rand"
)

const port = "8082"
const retryinterval = 5 * time.Second
const timeout = 2 * time.Second

var servaddr string
var servid int

var listener net.Listener
var shutdown bool = false

var trace bool = false
var trace_file *os.File = nil

const (
    FOLLOWER = iota
    CANDIDATE
    LEADER
)

type Server struct {
    Id int
    Addr string
    Up bool
}

type LogEntry struct {
    Term int
}

var term int			// current term number
var state int			// current state
var votedfor int		// candidate id we voted for
var logs []LogEntry		// log entries
var cluster map[int]Server	// servers in the cluster
var lastiteraction time.Time	// last time we heard from leader or voted for a candidate
var mutex sync.Mutex		// lock for shared data

func Start(ipaddr string, servers []string, debug bool) error {
    var err error

    term = 0
    votedfor = -1
    state = FOLLOWER
    lastiteraction = time.Now()
    cluster = make(map[int]Server)

    if (debug) {
	trace = true
	flags := (os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	trace_file, err = os.OpenFile("/tmp/raft.txt", flags, 0644)
	if err != nil {
	    return err
	}
    }

    raftrpc := new(RaftRPC)
    rpc.Register(raftrpc)

    listener, err = net.Listen("tcp", ipaddr + ":" + port)
    if err != nil {
	return err
    }

    for index, server := range servers {
	if server == ipaddr {
	    servaddr = server
	    servid = index
	    continue
	}
	addr := server + ":" + port
	svr := Server{Id: index, Addr: addr, Up: true}
	cluster[index] = svr
    }
    go rpc.Accept(listener)
    go ElectionTimer()

    return nil
}

func Stop() {
    if (trace) {
	trace_file.Close()
	trace = false
    }
    shutdown = true
    listener.Close()
}

func BecomeFollower(newterm int) {
    state = FOLLOWER
    term = newterm
    votedfor = -1
    lastiteraction = time.Now()
    DebugMsg("Became follower")
    go ElectionTimer()
}

func Heartbeat() {
    mutex.Lock()
    if state != LEADER {
	mutex.Unlock()
	return
    }
    currentterm := term
    mutex.Unlock()

    for _, server := range cluster {
	args := &AppendEntriesRequest{ Term: currentterm, LeaderId: servid }
	go func(server Server) {
	    var reply AppendEntriesReply

	    client, err := rpc.Dial("tcp", server.Addr)
	    if err != nil {
		return
	    }
	    defer client.Close()

	    err = client.Call("RaftRPC.AppendEntries", args, &reply)
	    if err != nil {
		return
	    }

	    mutex.Lock()
	    defer mutex.Unlock()

	    if reply.Term > currentterm {
		DebugMsg("Heartbeat canceled: Server " + server.Addr + " has a better term")
		BecomeFollower(reply.Term)
		return
	    }
	}(server)
    }
}

func BecomeLeader() {
    state = LEADER
    DebugMsg("Became leader")

    go func() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
	    if shutdown {
		return
	    }
	    Heartbeat()
	    <-ticker.C

	    mutex.Lock()
	    if state != LEADER {
		// heard from another peer with a better term.
		DebugMsg("Heartbeat canceled")
		mutex.Unlock()
		return
	    }
	    mutex.Unlock()
	}
    }()
}

func StartElection() {
    term++
    state = CANDIDATE
    votedfor = servid
    lastiteraction = time.Now()

    DebugMsg("Starting election")
    currentterm := term
    nvotes := 1

    for _, server := range cluster {
	go func(server Server) {
	    var reply VoteReply
	    args := &VoteRequest{ Term: currentterm, CandidateId: servid }

	    if !server.Up {
		return
	    }
	    client, err := rpc.Dial("tcp", server.Addr)

	    if err != nil {
		return
	    }
	    defer client.Close()

	    DebugMsg("Requesting vote from " + server.Addr)
	    err = client.Call("RaftRPC.RequestVote", args, &reply)
	    if err != nil {
		return
	    }

	    mutex.Lock()
	    defer mutex.Unlock()

	    if state != CANDIDATE {
		// if we already have enoght votes from other rpcs or if we
		// became a follower because other rpcs found out that there is
		// another noder with a better term.
		DebugMsg("Election canceled")
		return
	    }
	    if reply.Term > term {
		// another candidate won the election while we were requesting
		// votes.
		DebugMsg("Election canceled")
		BecomeFollower(reply.Term)
		return
	    }
	    if reply.Term == currentterm && reply.VoteGranted {
		nvotes++
		DebugMsg("Received vote from " + server.Addr)
		if nvotes <= (len(cluster) + 1) / 2 {
		    return
		}
		BecomeLeader()
		return
	    }
	}(server)
    }
    go ElectionTimer()
}

func DebugMsg(msg string) {
    if !trace {
	return
    }
    prefix := time.Now().Format("2006-01-02 15:04:05")
    suffix := fmt.Sprintf(" (term: %v, state: %v, votedfor: %v, servaddr: %v)", term, state, votedfor, servaddr)
    trace_file.WriteString(prefix + ": " + msg + suffix + "\n")
    trace_file.Sync()
}

// Time to wait before starting an election. Avoid starting an election at the
// same time as other peers by waiting a random amount of time before starting a
// new election. If we don't hear from the leader or vote for a candidate within
// this time, start an election. Also, bail out if we became the leader or our
// term has changed.
func ElectionTimer() {
    timeout := time.Duration(150 + rand.Intn(150)) * time.Millisecond

    mutex.Lock()
    currentterm := term
    DebugMsg("Election timer started: " + timeout.String())
    mutex.Unlock()

    ticker := time.NewTicker(10 * time.Millisecond)
    defer ticker.Stop()

    for {
	if shutdown {
	    return
	}
	<-ticker.C

	mutex.Lock()
	if (state != CANDIDATE && state != FOLLOWER) || term != currentterm {
	    // received enough votes from previous vote requests or found
	    // another peer with a better term (follower gets a request for vote
	    // from a leader in a higher term; this will trigger another
	    // BecomeFollower call that launches a new timer goroutine. return
	    // this one).
	    DebugMsg("Election timer canceled")
	    mutex.Unlock()
	    return
	}
	if time.Since(lastiteraction) >= timeout {
	    StartElection()
	    mutex.Unlock()
	    return
	}
	mutex.Unlock()
    }
}

// Remote Procedure Calls

type RaftRPC struct {}

type AppendEntriesRequest struct {
    Term int
    LeaderId int
}

type AppendEntriesReply struct {
    Term int
    Success bool
}

func (r *RaftRPC) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) error {
    mutex.Lock()
    defer mutex.Unlock()

    if shutdown {
	return nil
    }
    reply.Success = false

    if args.Term > term {
	DebugMsg("AppendEntries: better term from " + cluster[args.LeaderId].Addr)
	BecomeFollower(args.Term)
    }
    if args.Term == term {
	if state != FOLLOWER {
	    // candidate that find out that another peer won the election for
	    // this term.
	    BecomeFollower(args.Term)
	}
	reply.Success = true
	lastiteraction = time.Now()
    }
    reply.Term = term

    return nil
}

type VoteRequest struct {
    Term int
    CandidateId int
}

type VoteReply struct {
    Term int
    VoteGranted bool
}

func (r *RaftRPC) RequestVote(args *VoteRequest, reply *VoteReply) error {
    mutex.Lock()
    defer mutex.Unlock()

    if shutdown {
	return nil
    }
    reply.VoteGranted = false

    if args.Term > term {
	DebugMsg("RequestVote: better term from " + cluster[args.CandidateId].Addr)
	BecomeFollower(args.Term)
    }
    if args.Term == term && (votedfor == -1 || votedfor == args.CandidateId) {
	votedfor = args.CandidateId
	reply.VoteGranted = true
	lastiteraction = time.Now()
    }
    reply.Term = term

    return nil
}

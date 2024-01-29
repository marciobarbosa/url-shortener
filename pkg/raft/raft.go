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
    Command string
}

var term int			// current term number
var state int			// current state
var votedfor int		// candidate id we voted for
var commitidx int		// index of highest log entry known to be committed
var lastiteraction time.Time	// last time we heard from leader or voted for a candidate

var logs []LogEntry		// log entries
var cluster map[int]Server	// servers in the cluster
var server_nextidx map[int]int	// next log index to send to each server
var server_matchidx map[int]int	// highest log index known to be replicated on each server

var lastapplied int		// index of highest log entry applied to state machine
var CommitChan chan struct{}	// channel to notify ApplyLogEntries() that new entries can be committed
var ClientChan chan<- string	// channel to notify the application that new entries can be applied
var ApplyChangesCB func(string)	// callback to apply changes to the state machine

var mutex sync.Mutex		// lock for shared data

func Start(ipaddr string, servers []string, cli_chan chan<- string, cb func(string), debug bool) error {
    var err error

    term = 0
    votedfor = -1
    commitidx = -1
    state = FOLLOWER
    lastiteraction = time.Now()

    cluster = make(map[int]Server)
    server_nextidx = make(map[int]int)
    server_matchidx = make(map[int]int)

    lastapplied = -1
    ApplyChangesCB = cb
    ClientChan = cli_chan
    CommitChan = make(chan struct{}, 16)

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
    go ApplyLogEntries()

    return nil
}

func CreateLogEntry(command string) bool {
    mutex.Lock()
    defer mutex.Unlock()

    if state == LEADER {
	logs = append(logs, LogEntry{Term: term, Command: command})
	return true
    }
    return false
}

func Stop() {
    shutdown = true
    close(CommitChan)

    if (trace) {
	trace_file.Close()
	trace = false
    }
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

func PrintLog(ip string, slogs []LogEntry) {
    if len(slogs) == 0 {
	return
    }
    fmt.Println("Sending entries to " + ip + ":")

    for _, entry := range slogs {
	fmt.Printf("%d: %s\n", entry.Term, entry.Command)
    }
    fmt.Println()
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

	    mutex.Lock()
	    nextidx := server_nextidx[server.Id]
	    prevlogidx := nextidx - 1

	    prevlogterm := -1
	    if prevlogidx >= 0 {
		prevlogterm = logs[prevlogidx].Term
	    }
	    args.PrevLogTerm = prevlogterm
	    args.PrevLogIndex = prevlogidx
	    args.LeaderCommit = commitidx
	    args.Entries = logs[nextidx:]

	    PrintLog(server.Addr, args.Entries)
	    mutex.Unlock()

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
	    if state != LEADER || term != currentterm {
		return
	    }
	    if !reply.Success {
		server_nextidx[server.Id] = nextidx - 1
		return
	    }
	    server_nextidx[server.Id] = len(args.Entries) + nextidx
	    server_matchidx[server.Id] = server_nextidx[server.Id] - 1

	    current_commit := commitidx
	    for comm_i := commitidx + 1; comm_i < len(logs); comm_i++ {
		if logs[comm_i].Term != term {
		    continue
		}
		nreplicas := 1
		for _, server := range cluster {
		    if server_matchidx[server.Id] >= comm_i {
			nreplicas++
		    }
		    if nreplicas > (len(cluster) + 1) / 2 {
			commitidx = comm_i
			break
		    }
		}
	    }
	    if current_commit != commitidx {
		// notify the application that new entries have been committed
		CommitChan <- struct{}{}
	    }
	}(server)
    }
}

func BecomeLeader() {
    state = LEADER
    DebugMsg("Became leader")

    for _, server := range cluster {
	server_nextidx[server.Id] = len(logs)
	server_matchidx[server.Id] = -1
    }

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

func _GetLogState() (int, int) {
    if len(logs) == 0 {
	return -1, -1
    }
    return len(logs) - 1, logs[len(logs) - 1].Term
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

	    mutex.Lock()
	    lastlogidx, lastlogterm := _GetLogState()
	    mutex.Unlock()

	    args.LastLogIndex = lastlogidx
	    args.LastLogTerm = lastlogterm

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

func ApplyLogEntries() {
    for range CommitChan {
	var entries []LogEntry

	if shutdown {
	    break
	}
	mutex.Lock()
	last_applied := lastapplied
	if commitidx > lastapplied {
	    entries = logs[last_applied + 1 : commitidx + 1]
	    lastapplied = commitidx
	}
	mutex.Unlock()

	for _, entry := range entries {
	    ClientChan <- entry.Command
	}
    }
    DebugMsg("ApplyLogEntries stopped")
}

func ApplyLogEntriesFollower() {
    var entries []LogEntry

    if shutdown {
	return
    }
    mutex.Lock()
    last_applied := lastapplied
    if commitidx > lastapplied {
	entries = logs[last_applied + 1 : commitidx + 1]
	lastapplied = commitidx
    }
    mutex.Unlock()

    for _, entry := range entries {
	ApplyChangesCB(entry.Command)
    }
}

// Remote Procedure Calls

type RaftRPC struct {}

type AppendEntriesRequest struct {
    Term int
    LeaderId int
    PrevLogTerm int
    PrevLogIndex int
    LeaderCommit int
    Entries []LogEntry
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
	lastiteraction = time.Now()

	if args.PrevLogIndex == -1 ||
	    (args.PrevLogIndex < len(logs) && logs[args.PrevLogIndex].Term == args.PrevLogTerm) {
	    // append entries to log
	    reply.Success = true

	    insertidx := args.PrevLogIndex + 1
	    newidx := 0
	    for {
		if insertidx >= len(logs) || newidx >= len(args.Entries) {
		    break
		}
		if logs[insertidx].Term != args.Entries[newidx].Term {
		    break
		}
		insertidx++
		newidx++
	    }
	    PrintLog(cluster[args.LeaderId].Addr, args.Entries[newidx:])

	    if newidx < len(args.Entries) {
		// append new entries
		logs = append(logs[:insertidx], args.Entries[newidx:]...)
	    }
	    if args.LeaderCommit > commitidx {
		// commit new entries
		commitidx = min(args.LeaderCommit, len(logs) - 1)
		// notify the application that new entries have been committed
		ApplyLogEntriesFollower()
		//CommitChan <- struct{}{}
	    }
	    //DebugMsg("AppendEntries succeeded")
	}
    }
    reply.Term = term

    return nil
}

type VoteRequest struct {
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

type VoteReply struct {
    Term        int
    VoteGranted bool
}

func _CandidateLogOK(args *VoteRequest) bool {
    lastlogidx, lastlogterm := _GetLogState()

    if args.LastLogTerm > lastlogterm {
	return true
    }
    if args.LastLogTerm == lastlogterm && args.LastLogIndex >= lastlogidx {
	return true
    }
    return false
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
    if args.Term == term &&
	(votedfor == -1 || votedfor == args.CandidateId) && _CandidateLogOK(args) {
	votedfor = args.CandidateId
	reply.VoteGranted = true
	lastiteraction = time.Now()
    }
    reply.Term = term

    return nil
}

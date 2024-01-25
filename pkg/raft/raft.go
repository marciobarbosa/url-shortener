package raft

import (
    "net"
    "time"
    "net/rpc"
    "github.com/marciobarbosa/url-shortener/internal/raftrpc"
)

const port = "8082"
const retryinterval = 5 * time.Second
const timeout = 2 * time.Second

var listener net.Listener
var shutdown bool = false

type Server struct {
    Id int
    Addr string
    Up bool
}
var cluster []Server

func Heartbeat() {
    for {
	for _, server := range cluster {
	    if shutdown {
		return
	    }
	    conn, err := net.DialTimeout("tcp", server.Addr, timeout)
	    if err != nil {
		server.Up = false
		continue
	    }
	    server.Up = true
	    client := rpc.NewClient(conn)

	    args := &raftrpc.AppendEntriesRequest{}
	    var result raftrpc.AppendEntriesReply

	    err = client.Call("RaftRPC.AppendEntries", args, &result)
	    if err != nil {
		server.Up = false
	    }
	    client.Close()
	    conn.Close()
	}
	time.Sleep(retryinterval)
    }
}

func Start(ipaddr string, servers []string) error {
    var err error

    raftrpc := new(raftrpc.RaftRPC)
    rpc.Register(raftrpc)

    listener, err = net.Listen("tcp", ipaddr + ":" + port)
    if err != nil {
	return err
    }

    for index, server := range servers {
	if server == ipaddr {
	    continue
	}
	addr := server + ":" + port
	svr := Server{Id: index, Addr: addr, Up: false}
	cluster = append(cluster, svr)
    }
    go rpc.Accept(listener)
    go Heartbeat()

    return nil
}

func Stop() {
    shutdown = true
    listener.Close()
}

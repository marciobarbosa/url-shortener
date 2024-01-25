package raftrpc

import (
    "fmt"
)

type RaftRPC struct {}

type AppendEntriesRequest struct {
}

type AppendEntriesReply struct {
}

func (r *RaftRPC) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) error {
    fmt.Println("AppendEntries")
    return nil
}

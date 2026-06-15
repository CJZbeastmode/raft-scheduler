package raft

import (
    "net/rpc"
)

// Peer represents a remote Raft node reachable over TCP
type Peer struct {
    addr string // "host:port"
}

func NewPeer(addr string) *Peer {
    return &Peer{addr: addr}
}

// Call makes a synchronous RPC call to this peer
func (p *Peer) Call(method string, args, reply interface{}) bool {
    client, err := rpc.Dial("tcp", p.addr)
    if err != nil {
        return false
    }
    defer client.Close()
    err = client.Call(method, args, reply)
    return err == nil
}

// RPCAdapter wraps *Raft so its handlers satisfy net/rpc's required signature:
// func (t *T) MethodName(args *A, reply *R) error
// Register with: server.RegisterName("Raft", &raft.RPCAdapter{Rf: rf})
type RPCAdapter struct {
    Rf *Raft
}

func (a *RPCAdapter) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
    a.Rf.RequestVote(args, reply)
    return nil
}

func (a *RPCAdapter) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
    a.Rf.AppendEntries(args, reply)
    return nil
}

func (a *RPCAdapter) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
    a.Rf.InstallSnapshot(args, reply)
    return nil
}
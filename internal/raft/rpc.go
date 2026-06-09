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
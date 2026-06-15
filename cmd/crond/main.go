package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"market-intel/internal/raft"
	"market-intel/internal/scheduler"
	"market-intel/internal/store"
)

// env vars:
//
//	BIND_ADDR  — TCP address to listen on, e.g. ":8001"
//	PEERS      — comma-separated list of ALL peer addresses in order, e.g. "host1:8001,host2:8002,host3:8003"
//	ME         — 0-indexed position of this node in PEERS, e.g. "0"
func main() {
	bindAddr := mustEnv("BIND_ADDR")
	peersEnv := mustEnv("PEERS")
	meStr := mustEnv("ME")

	me, err := strconv.Atoi(meStr)
	if err != nil {
		log.Fatalf("ME must be an integer, got %q", meStr)
	}

	// build peer list — Raft needs all peers (including self) in the same order on every node
	addrs := strings.Split(peersEnv, ",")
	peers := make([]*raft.Peer, len(addrs))
	for i, addr := range addrs {
		peers[i] = raft.NewPeer(strings.TrimSpace(addr))
	}
	if me < 0 || me >= len(peers) {
		log.Fatalf("ME=%d out of range for %d peers", me, len(peers))
	}

	// start the TCP listener before creating Raft so peers can dial us immediately
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", bindAddr, err)
	}
	log.Printf("node %d listening on %s", me, bindAddr)

	// Raft and store share this channel
	applyCh := make(chan raft.ApplyMsg)

	// in-memory persister — survives the process lifetime but not a crash
	// TODO Session 8: replace with a disk-backed persister for real durability
	persister := raft.MakePersister()

	rf := raft.Make(peers, me, persister, applyCh)

	// register Raft with the RPC server using the service name "Raft" so that
	// Peer.Call("Raft.RequestVote", ...) routes correctly
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &raft.RPCAdapter{Rf: rf}); err != nil {
		log.Fatalf("rpc register: %v", err)
	}
	go srv.Accept(ln)

	st := store.Make(rf, persister, applyCh)

	sc := scheduler.Make(rf, st, scheduler.NoopExecutor{})

	// wait for the cluster to elect a leader, then catch up any missed jobs
	go func() {
		for {
			_, isLeader := rf.GetState()
			if isLeader {
				log.Printf("node %d is leader — running missed job reconciliation", me)
				sc.ReconcileMissedJobs()
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// block until SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("node %d shutting down (%v)", me, sig)

	sc.Kill()
	rf.Kill()
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env var %s is required", key)
	}
	return v
}

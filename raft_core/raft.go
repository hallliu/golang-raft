package raft_core

import (
	"math/rand"
	"time"
	"transporter"
)

func getElectionTimeout() time.Duration {
	timeoutOffset := rand.Int63n(200)
	return (150 + timeoutOffset) * time.Millisecond
}

func MakeRaftNode(hostname string, hostnames []string, ownTransport Transporter, commitChannel chan []byte) (result *RaftNode) {
	resultNode := &RaftNode{
		MsgTransport:  ownTransport,
		CommitChannel: commitChannel,
		hostnames:     hostnames,
		serverId:      hostname,
		currentRole:   clusterFollower,
	}
}

/*
This function kicks off the first timeout to begin leader election, then
runs in a perpetual loop waiting for input from the transport.
*/
func (node *RaftNode) run() {
	node.currentTimeout = time.After(getElectionTimeout())
	for {
		select {
		case timedOut := <-node.currentTimeout:
			node.handleTimeout()
		case message := <-node.MsgTransport.Recv:
			node.handleMessage(message)
		}
	}
}

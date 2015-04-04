package raft_core

import (
	"math/rand"
	"raft/transporter"
	"time"
)

var _ = transporter.MakeChannelTransporters // Remove after use is found

func getElectionTimeout() time.Duration {
	timeoutOffset := rand.Int63n(200)
	return time.Duration(150+timeoutOffset) * time.Millisecond
}

func MakeRaftNode(hostname string, hostnames []string, ownTransport transporter.Transporter, commitChannel chan []byte) (result *RaftNode) {
	result = &RaftNode{
		MsgTransport:  ownTransport,
		CommitChannel: commitChannel,
		hostnames:     hostnames,
		serverId:      hostname,
		currentRole:   clusterFollower,
	}
	return
}

/*
This function kicks off the first timeout to begin leader election, then
runs in a perpetual loop waiting for input from the transport.
*/
func (node *RaftNode) run() {
	node.currentTimeout = time.After(getElectionTimeout())
	for {
		select {
		case <-node.currentTimeout:
			node.handleTimeout()
		case message := <-node.MsgTransport.Recv:
			node.handleMessage(message)
		}
	}
}

func (node *RaftNode) handleTimeout()                             {}
func (node *RaftNode) handleMessage(message *transporter.Message) {}

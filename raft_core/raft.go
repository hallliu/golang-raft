package raft_core

import (
	"github.com/hallliu/golang-raft/transporter"
	"time"
)

func MakeRaftNode(hostname string, hostnames []string, ownTransport transporter.Transporter, commitChannel chan []byte) (result *RaftNode) {
	peerNames := make([]string, len(hostnames)-1, len(hostnames)-1)
	for _, name := range hostnames {
		if name != hostname {
			peerNames = append(peerNames, name)
		}
	}

	result = &RaftNode{
		MsgTransport:  ownTransport,
		CommitChannel: commitChannel,
		peernames:     peerNames,
		serverId:      hostname,
		currentRole:   clusterFollower,
		messageLog:    []logEntry{logEntry{-1, []byte{}}},
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

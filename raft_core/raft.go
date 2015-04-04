package raft_core

import (
	"encoding/json"
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

func (node *RaftNode) handleTimeout() {
	switch {
	case node.currentRole == clusterFollower:
		node.beginCandidacy()
	case node.currentRole == leaderCandidate:
		node.beginCandidacy()
	case node.currentRole == clusterLeader:
		node.heartBeat()
	}
}

// heartBeat sends a heartbeat message to all the followers.
// This function prevents followers' election timeouts from expiring.
func (node *RaftNode) heartBeat() {
	for _, hostname := range node.hostnames {
		prevLogIndex := node.nextIndex[hostname] - 1
		lastLogTerm := node.messageLog[prevLogIndex].term

		heartBeatCommand := appendEntriesCmd{
			commandType:       appendEntriesType,
			term:              node.currentTerm,
			leaderId:          node.serverId,
			prevLogIndex:      prevLogIndex,
			prevLogTerm:       lastLogTerm,
			entries:           node.messageLog[prevLogIndex+1:],
			leaderCommitIndex: node.commitIndex,
		}
		heartBeatJson, _ := json.Marshal(heartBeatCommand)

		heartBeatMessage := transporter.Message{
			Destination: hostname,
			Source:      node.serverId,
			Command:     heartBeatJson,
		}

		node.MsgTransport.Send <- &heartBeatMessage
	}
}

func (node *RaftNode) handleMessage(message *transporter.Message) {}

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
	for _, hostname := range node.peernames {
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

		sendMessage(node.serverId, []string{hostname}, heartBeatCommand, node.MsgTransport.Send)
	}
	node.currentTimeout = time.After(20 * time.Millisecond)
}

// beginCandidacy runs when the election timeout expires.
// This can happen either when the leader goes down or when an election is unsuccessful.
// The server puts itself into a candidate state and sends out RequestVote messages to all other servers.
func (node *RaftNode) beginCandidacy() {
	node.currentLeader = ""
	node.currentRole = leaderCandidate
	node.currentTerm += 1

	reqVoteCmd := requestVoteCmd{
		commandType:  requestVoteType,
		term:         node.currentTerm,
		candidateId:  node.serverId,
		lastLogIndex: len(node.messageLog) - 1,
		lastLogTerm:  node.messageLog[len(node.messageLog)-1].term,
	}
	sendMessage(node.serverId, node.peernames, reqVoteCmd, node.MsgTransport.Send)

	node.votedFor = node.serverId
	node.numVotes = 1
	node.currentTimeout = time.After(getElectionTimeout())
}

func sendMessage(source string, destinations []string, command interface{}, channel chan<- *transporter.Message) {
	cmdJson, _ := json.Marshal(command)
	for _, destination := range destinations {
		cmdMessage := transporter.Message{
			Destination: destination,
			Source:      source,
			Command:     cmdJson,
		}
		channel <- &cmdMessage
	}
}

func (node *RaftNode) handleMessage(message *transporter.Message) {}

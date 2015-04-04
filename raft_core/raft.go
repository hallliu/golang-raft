package raft_core

import (
	"encoding/json"
	"math/rand"
	"raft/transporter"
	"time"
)

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
	var cmdType raftCommandType
	switch command.(type) {
	case appendEntriesCmd:
		cmdType = appendEntriesType
	case requestVoteCmd:
		cmdType = requestVoteType
	case appendEntriesReply:
		cmdType = appendEntriesReplyType
	case requestVoteReply:
		cmdType = requestVoteReplyType
	default:
		panic("Invalid command type")
	}

	cmdJson, _ := json.Marshal(command)
	wrappedCommand := commandWrapper{cmdType, cmdJson}
	wrappedJson, _ := json.Marshal(wrappedCommand)

	for _, destination := range destinations {
		cmdMessage := transporter.Message{
			Destination: destination,
			Source:      source,
			Command:     wrappedJson,
		}
		channel <- &cmdMessage
	}
}

func (node *RaftNode) handleMessage(message *transporter.Message) {
	var wrappedCommand commandWrapper
	json.Unmarshal(message.Command, wrappedCommand)
	switch wrappedCommand.commandType {
	case appendEntriesType:
		var cmd appendEntriesCmd
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		node.handleAppendEntries(cmd, message.Source)
	case requestVoteType:
		var cmd requestVoteCmd
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		node.handleRequestVote(cmd)
	case appendEntriesReplyType:
		var cmd appendEntriesReply
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		node.handleAppendEntriesReply(cmd)
	case requestVoteReplyType:
		var cmd requestVoteReply
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		node.handleRequestVoteReply(cmd)
	}
}

func (node *RaftNode) handleAppendEntries(cmd appendEntriesCmd, source string) {
	if node.currentTerm <= cmd.term {
		node.becomeFollower(cmd.term)
	}

	if node.currentTerm > cmd.term {
		node.rejectAppendEntries(cmd, source)
		return
	}

	node.currentLeader = source

	// Own log too short
	if len(node.messageLog) <= cmd.prevLogIndex {
		node.rejectAppendEntries(cmd, source)
		return
	}

	// Terms don't match up at prevLogIndex
	supposedPreviousTerm := node.messageLog[cmd.prevLogIndex].term
	if supposedPreviousTerm != cmd.prevLogTerm {
		node.messageLog = node.messageLog[:cmd.prevLogIndex]
		node.rejectAppendEntries(cmd, source)
		return
	}

	newEntriesIndex := 0
	for idx, entry := range node.messageLog[cmd.prevLogIndex+1:] {
		newEntriesIndex = idx - (cmd.prevLogIndex + 1)
		if newEntriesIndex >= len(cmd.entries) {
			break
		}
		if entry.term != cmd.entries[newEntriesIndex].term {
			node.messageLog = node.messageLog[:idx]
			break
		}
	}

	node.messageLog = append(node.messageLog, cmd.entries[newEntriesIndex:]...)

	if cmd.leaderCommitIndex > node.commitIndex {
		var newCommitIndex int
		if cmd.leaderCommitIndex < len(node.messageLog)-1 {
			newCommitIndex = cmd.leaderCommitIndex
		} else {
			newCommitIndex = len(node.messageLog) - 1
		}
		for _, entry := range node.messageLog[node.commitIndex+1 : newCommitIndex+1] {
			node.CommitChannel <- entry.command
		}
		node.commitIndex = newCommitIndex
	}
}

func (node *RaftNode) handleRequestVote(cmd requestVoteCmd)            {}
func (node *RaftNode) handleAppendEntriesReply(cmd appendEntriesReply) {}
func (node *RaftNode) handleRequestVoteReply(cmd requestVoteReply)     {}

// becomeFollower reverts the server back to a follower state.
// It is called when an AppendEntries message with a higher term than one's own is received.
func (node *RaftNode) becomeFollower(term int) {
	node.currentRole = clusterFollower
	if term > node.currentTerm {
		node.votedFor = ""
	}
	node.currentTerm = term
	node.currentTimeout = time.After(getElectionTimeout())
}

func (node *RaftNode) rejectAppendEntries(origCommand appendEntriesCmd, origSender string) {
	rejection := appendEntriesReply{
		originalMessage: origCommand,
		term:            node.currentTerm,
		success:         false,
	}
	sendMessage(node.serverId, []string{origSender}, rejection, node.MsgTransport.Send)
}

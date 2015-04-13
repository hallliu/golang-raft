// File for the message handlers that act upon the raft node and the helper functions that they call

package raft_core

import (
	"encoding/json"
	"fmt"
	"github.com/hallliu/golang-raft/transporter"
	"sort"
	"time"
)

func (node *RaftNode) handleTimeout() {
	node.debugLog("Timed out at term %d. Current status is %s\n", node.serverId, node.currentTerm, node.currentRole)
	switch {
	case node.currentRole == clusterFollower:
		node.beginCandidacy()
	case node.currentRole == leaderCandidate:
		node.beginCandidacy()
	case node.currentRole == clusterLeader:
		node.heartBeat()
	}
}

func (node *RaftNode) handleAppendEntries(cmd appendEntriesCmd, source string) {
	node.debugLog("Received appendEntries: %+v\n", cmd)

	if node.currentTerm <= cmd.term {
		node.debugLog(
			"Reverting to follower because own term is %d and rcvd term is %d",
			node.currentTerm,
			cmd.term,
		)

		node.becomeFollower(cmd.term)
	}

	if node.currentTerm > cmd.term {
		node.debugLog(
			"Rejecting appendEntries because own term is %d and rcvd term is %d",
			node.currentTerm,
			cmd.term,
		)
		node.replyAppendEntries(cmd, source, false)
		return
	}

	node.currentLeader = source

	// Own log too short
	if len(node.messageLog) <= cmd.prevLogIndex {
		node.debugLog(
			"Rejecting appendEntries because own log has %d entries and cmd expects %d",
			len(node.messageLog),
			cmd.prevLogIndex,
		)
		node.replyAppendEntries(cmd, source, false)
		return
	}

	// Terms don't match up at prevLogIndex
	supposedPreviousTerm := node.messageLog[cmd.prevLogIndex].term
	if supposedPreviousTerm != cmd.prevLogTerm {
		node.debugLog(
			"Rejecting appendEntries because rcvd term at position %d is %d"+
				"and own term at same position is %d",
			cmd.prevLogIndex,
			cmd.prevLogTerm,
			supposedPreviousTerm,
		)
		node.messageLog = node.messageLog[:cmd.prevLogIndex]
		node.replyAppendEntries(cmd, source, false)
		return
	}

	newEntriesIndex := 0
	for idx, entry := range node.messageLog[cmd.prevLogIndex+1:] {
		newEntriesIndex = idx - (cmd.prevLogIndex + 1)
		if newEntriesIndex >= len(cmd.entries) {
			node.debugLog("Reached end of received entries without finding anything new")
			break
		}
		if entry.term != cmd.entries[newEntriesIndex].term {
			node.debugLog(
				"Term mismatch at index %d of own log and %d of rcvd entries. Overwriting our own.",
				idx,
				newEntriesIndex,
			)
			node.messageLog = node.messageLog[:idx]
			break
		}
	}

	node.debugLog("About to append %d entries to own log", len(cmd.entries)-newEntriesIndex)
	node.messageLog = append(node.messageLog, cmd.entries[newEntriesIndex:]...)

	if cmd.leaderCommitIndex > node.commitIndex {
		var newCommitIndex int
		if cmd.leaderCommitIndex < len(node.messageLog)-1 {
			newCommitIndex = cmd.leaderCommitIndex
		} else {
			newCommitIndex = len(node.messageLog) - 1
		}
		for idx, entry := range node.messageLog[node.commitIndex+1 : newCommitIndex+1] {
			node.debugLog("Committing command %s at index %d", idx, entry.command)
			node.CommitChannel <- entry.command
		}
		node.commitIndex = newCommitIndex
	}

	node.replyAppendEntries(cmd, source, true)
	return
}

func (node *RaftNode) handleRequestVote(cmd requestVoteCmd) {
	if cmd.term < node.currentTerm {
		node.replyRequestVote(cmd.candidateId, false)
	}

	// Reset the timeout because at this point, this node knows that there is a viable candidate.
	node.currentTimeout = time.After(getElectionTimeout())

	if cmd.term > node.currentTerm {
		node.becomeFollower(cmd.term)
	}

	hasNotVoted := (node.votedFor == "")
	candidateTermUpToDate := node.messageLog[len(node.messageLog)-1].term <= cmd.lastLogTerm
	candidateIndexUpToDate := len(node.messageLog)-1 <= cmd.lastLogIndex

	if hasNotVoted && candidateTermUpToDate && candidateIndexUpToDate {
		node.replyRequestVote(cmd.candidateId, true)
		node.votedFor = cmd.candidateId
	} else {
		node.replyRequestVote(cmd.candidateId, false)
	}
	return
}

func (node *RaftNode) handleAppendEntriesReply(cmd appendEntriesReply, source string) {
	if cmd.term > node.currentTerm {
		node.becomeFollower(cmd.term)
	}

	// Drop the message if it got delayed from earlier and this node is no longer a leader.
	if node.currentRole != clusterLeader {
		return
	}

	if !cmd.success {
		node.nextIndex[source] -= 1
		retryAppendEntry := appendEntriesCmd{
			term:              node.currentTerm,
			leaderId:          node.serverId,
			prevLogIndex:      node.nextIndex[source] - 1,
			prevLogTerm:       node.messageLog[node.nextIndex[source]-1].term,
			entries:           node.messageLog[node.nextIndex[source]:],
			leaderCommitIndex: node.commitIndex,
		}
		sendMessage(node.serverId, []string{source}, retryAppendEntry, node.MsgTransport.Send)
		return
	}

	if node.matchIndex[source] < cmd.originalMessage.prevLogIndex+len(cmd.originalMessage.entries) {
		node.matchIndex[source] = cmd.originalMessage.prevLogIndex + len(cmd.originalMessage.entries)
	}
	node.nextIndex[source] = node.matchIndex[source] + 1

	// Check if there's anything that can be committed
	matchedIndices := make([]int, 0, len(node.matchIndex))
	for _, matchIdx := range node.matchIndex {
		matchedIndices = append(matchedIndices, matchIdx)
	}
	sort.Ints(matchedIndices)
	tentativeCommitIndex := matchedIndices[len(node.peernames)/2]
	if tentativeCommitIndex > node.commitIndex {
		if node.messageLog[tentativeCommitIndex].term == node.currentTerm {
			for _, entry := range node.messageLog[node.commitIndex+1 : tentativeCommitIndex+1] {
				node.CommitChannel <- entry.command
			}
			node.commitIndex = tentativeCommitIndex
		}
	}
	return
}
func (node *RaftNode) handleRequestVoteReply(cmd requestVoteReply) {
	if cmd.term > node.currentTerm {
		node.becomeFollower(cmd.term)
		return
	}

	if cmd.term < node.currentTerm || !(node.currentRole != leaderCandidate) {
		return
	}

	if cmd.voteGranted {
		node.numVotes += 1
		if node.numVotes >= len(node.peernames)/2 {
			node.becomeLeader()
		}
	}
	return
}

func (node *RaftNode) becomeLeader() {
	node.currentTimeout = nil
	node.currentRole = clusterLeader

	node.nextIndex = make(map[string]int)
	for _, peerName := range node.peernames {
		node.nextIndex[peerName] = len(node.messageLog)
	}

	node.matchIndex = make(map[string]int)
	for _, peerName := range node.peernames {
		node.matchIndex[peerName] = 0
	}

	node.currentLeader = node.serverId
	node.messageLog = append(node.messageLog, logEntry{term: node.currentTerm, command: []byte{}})
	node.heartBeat()
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
		node.handleAppendEntriesReply(cmd, message.Source)
	case requestVoteReplyType:
		var cmd requestVoteReply
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		node.handleRequestVoteReply(cmd)
	}
}

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

func (node *RaftNode) replyAppendEntries(origCommand appendEntriesCmd, origSender string, success bool) {
	reply := appendEntriesReply{
		originalMessage: origCommand,
		term:            node.currentTerm,
		success:         success,
	}
	sendMessage(node.serverId, []string{origSender}, reply, node.MsgTransport.Send)
}

func (node *RaftNode) replyRequestVote(origSender string, voteGranted bool) {
	reply := requestVoteReply{
		term:        node.currentTerm,
		voteGranted: voteGranted,
	}

	sendMessage(node.serverId, []string{origSender}, reply, node.MsgTransport.Send)
}

func (node *RaftNode) debugLog(format string, args ...interface{}) {
	node.logger.Debug("Node %s: "+format, node.serverId, args...)
}

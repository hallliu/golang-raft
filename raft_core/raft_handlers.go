// File for the message handlers that act upon the raft node and the helper functions that they call

package raft_core

import (
	"encoding/json"
	"fmt"
	"github.com/hallliu/golang-raft/transporter"
	"sort"
	"time"
)

var _ = fmt.Errorf

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

	if node.currentTerm <= cmd.Term {
		node.debugLog(
			"Reverting to follower because own term is %d and rcvd term is %d",
			node.currentTerm,
			cmd.Term,
		)

		node.becomeFollower(cmd.Term)
	}

	if node.currentTerm > cmd.Term {
		node.debugLog(
			"Rejecting appendEntries because own term is %d and rcvd term is %d",
			node.currentTerm,
			cmd.Term,
		)
		node.replyAppendEntries(cmd, source, false)
		return
	}

	node.currentLeader = source

	// Own log too short
	if len(node.messageLog) <= cmd.PrevLogIndex {
		node.debugLog(
			"Rejecting appendEntries because own log has %d entries and cmd expects %d",
			len(node.messageLog),
			cmd.PrevLogIndex,
		)
		node.replyAppendEntries(cmd, source, false)
		return
	}

	// Terms don't match up at prevLogIndex
	supposedPreviousTerm := node.messageLog[cmd.PrevLogIndex].Term
	if supposedPreviousTerm != cmd.PrevLogTerm {
		node.debugLog(
			"Rejecting appendEntries because rcvd term at position %d is %d"+
				"and own term at same position is %d",
			cmd.PrevLogIndex,
			cmd.PrevLogTerm,
			supposedPreviousTerm,
		)
		node.messageLog = node.messageLog[:cmd.PrevLogIndex]
		node.replyAppendEntries(cmd, source, false)
		return
	}

	newEntriesIndex := 0
	for idx, entry := range node.messageLog[cmd.PrevLogIndex+1:] {
		newEntriesIndex = idx - (cmd.PrevLogIndex + 1)
		if newEntriesIndex >= len(cmd.Entries) {
			node.debugLog("Reached end of received entries without finding anything new")
			break
		}
		if entry.Term != cmd.Entries[newEntriesIndex].Term {
			node.debugLog(
				"Term mismatch at index %d of own log and %d of rcvd entries. Overwriting our own.",
				idx,
				newEntriesIndex,
			)
			node.messageLog = node.messageLog[:idx]
			break
		}
	}

	node.debugLog("About to append %d entries to own log", len(cmd.Entries)-newEntriesIndex)
	node.messageLog = append(node.messageLog, cmd.Entries[newEntriesIndex:]...)

	if cmd.LeaderCommitIndex > node.commitIndex {
		var newCommitIndex int
		if cmd.LeaderCommitIndex < len(node.messageLog)-1 {
			newCommitIndex = cmd.LeaderCommitIndex
		} else {
			newCommitIndex = len(node.messageLog) - 1
		}
		for idx, entry := range node.messageLog[node.commitIndex+1 : newCommitIndex+1] {
			node.debugLog("Committing command %s at index %d", idx, entry.Command)
			node.CommitChannel <- entry.Command
		}
		node.commitIndex = newCommitIndex
	}

	node.replyAppendEntries(cmd, source, true)
	return
}

func (node *RaftNode) handleRequestVote(cmd requestVoteCmd) {
	node.debugLog("Received requestVote: %+v\n", cmd)
	if cmd.Term < node.currentTerm {
		node.debugLog("requestVote denied: own term is %d, rcvd term is %d", node.currentTerm, cmd.Term)
		node.replyRequestVote(cmd.CandidateId, false)
	}

	// Reset the timeout because at this point, this node knows that there is a viable candidate.
	node.currentTimeout = time.After(getElectionTimeout())

	if cmd.Term > node.currentTerm {
		node.debugLog("Becoming follower because received term %d and own term is %d", cmd.Term, node.currentTerm)
		node.becomeFollower(cmd.Term)
	}

	hasNotVoted := (node.votedFor == "")
	candidateTermUpToDate := node.messageLog[len(node.messageLog)-1].Term <= cmd.LastLogTerm
	candidateIndexUpToDate := len(node.messageLog)-1 <= cmd.LastLogIndex

	if hasNotVoted && candidateTermUpToDate && candidateIndexUpToDate {
		node.debugLog("Voting for %s for term %d", cmd.CandidateId, cmd.Term)
		node.replyRequestVote(cmd.CandidateId, true)
		node.votedFor = cmd.CandidateId
	} else {
		node.debugLog("Rejecting requestVote because...")
		if !hasNotVoted {
			node.debugLog("Already voted for %s", node.votedFor)
		}
		if !candidateTermUpToDate {
			node.debugLog("Candidate's last log term is %d, but our own is %d",
				cmd.LastLogTerm,
				node.messageLog[len(node.messageLog)-1].Term,
			)
		}
		if !candidateIndexUpToDate {
			node.debugLog("Candidate's last log index is %d, but our log's last index is %d", cmd.LastLogIndex, len(node.messageLog)-1)
		}

		node.replyRequestVote(cmd.CandidateId, false)
	}
	return
}

func (node *RaftNode) handleAppendEntriesReply(cmd appendEntriesReply, source string) {
	node.debugLog("Got appendEntries reply %+v from %s", cmd, source)

	if cmd.Term > node.currentTerm {
		node.debugLog("Reverting to follower because received message with term %d and our own term is %d",
			cmd.Term,
			node.currentTerm,
		)
		node.becomeFollower(cmd.Term)
	}

	// Drop the message if it got delayed from earlier and this node is no longer a leader.
	if node.currentRole != clusterLeader {
		node.debugLog("Finished handling this appendEntries reply because we're no longer a leader")
		return
	}

	if !cmd.Success {
		node.debugLog("Received rejected appendEntries. Resending")
		node.nextIndex[source] -= 1
		retryAppendEntry := appendEntriesCmd{
			Term:              node.currentTerm,
			LeaderId:          node.serverId,
			PrevLogIndex:      node.nextIndex[source] - 1,
			PrevLogTerm:       node.messageLog[node.nextIndex[source]-1].Term,
			Entries:           node.messageLog[node.nextIndex[source]:],
			LeaderCommitIndex: node.commitIndex,
		}
		sendMessage(node.serverId, []string{source}, retryAppendEntry, node.MsgTransport.Send)
		return
	}

	if node.matchIndex[source] < cmd.OriginalMessage.PrevLogIndex+len(cmd.OriginalMessage.Entries) {
		node.matchIndex[source] = cmd.OriginalMessage.PrevLogIndex + len(cmd.OriginalMessage.Entries)
		node.debugLog("Node %s's match index increased to %d", node.matchIndex[source])
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
		node.debugLog("About to commit %d entries", tentativeCommitIndex-node.commitIndex)
		if node.messageLog[tentativeCommitIndex].Term == node.currentTerm {
			for _, entry := range node.messageLog[node.commitIndex+1 : tentativeCommitIndex+1] {
				node.CommitChannel <- entry.Command
			}
			node.commitIndex = tentativeCommitIndex
		}
	}
	return
}
func (node *RaftNode) handleRequestVoteReply(cmd requestVoteReply, source string) {
	node.debugLog("Got reply of requestVote: %+v from %s.", cmd, source)
	if cmd.Term > node.currentTerm {
		node.debugLog("Reverting to follower because received message with term %d and our own term is %d",
			cmd.Term,
			node.currentTerm,
		)
		node.becomeFollower(cmd.Term)
		return
	}

	if cmd.Term < node.currentTerm || !(node.currentRole != leaderCandidate) {
		node.debugLog("Ignoring reply because term is out of date (%d) or we are no longer a candidate", cmd.Term)
		return
	}

	if cmd.VoteGranted {
		node.numVotes += 1
		node.debugLog("Vote received. Now have %d votes out of %d necessary", node.numVotes, len(node.peernames)/2)
		if node.numVotes >= len(node.peernames)/2 {
			node.debugLog("Got enough votes. Becoming leader at term %d", node.currentTerm)
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
	node.messageLog = append(node.messageLog, logEntry{Term: node.currentTerm, Command: []byte{}})
	node.heartBeat()
}

// heartBeat sends a heartbeat message to all the followers.
// This function prevents followers' election timeouts from expiring.
func (node *RaftNode) heartBeat() {
	for _, hostname := range node.peernames {
		prevLogIndex := node.nextIndex[hostname] - 1
		lastLogTerm := node.messageLog[prevLogIndex].Term

		heartBeatCommand := appendEntriesCmd{
			Term:              node.currentTerm,
			LeaderId:          node.serverId,
			PrevLogIndex:      prevLogIndex,
			PrevLogTerm:       lastLogTerm,
			Entries:           node.messageLog[prevLogIndex+1:],
			LeaderCommitIndex: node.commitIndex,
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
		Term:         node.currentTerm,
		CandidateId:  node.serverId,
		LastLogIndex: len(node.messageLog) - 1,
		LastLogTerm:  node.messageLog[len(node.messageLog)-1].Term,
	}
	sendMessage(node.serverId, node.peernames, reqVoteCmd, node.MsgTransport.Send)

	node.votedFor = node.serverId
	node.numVotes = 1
	node.currentTimeout = time.After(getElectionTimeout())
}

func (node *RaftNode) handleMessage(message *transporter.Message) {
	var wrappedCommand commandWrapper
	json.Unmarshal(message.Command, &wrappedCommand)
	switch wrappedCommand.CommandType {
	case appendEntriesType:
		var cmd appendEntriesCmd
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		node.handleAppendEntries(cmd, message.Source)
	case requestVoteType:
		var cmd requestVoteCmd
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		node.handleRequestVote(cmd)
	case appendEntriesReplyType:
		var cmd appendEntriesReply
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		node.handleAppendEntriesReply(cmd, message.Source)
	case requestVoteReplyType:
		var cmd requestVoteReply
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		node.handleRequestVoteReply(cmd, message.Source)
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
		OriginalMessage: origCommand,
		Term:            node.currentTerm,
		Success:         success,
	}
	sendMessage(node.serverId, []string{origSender}, reply, node.MsgTransport.Send)
}

func (node *RaftNode) replyRequestVote(origSender string, voteGranted bool) {
	reply := requestVoteReply{
		Term:        node.currentTerm,
		VoteGranted: voteGranted,
	}

	sendMessage(node.serverId, []string{origSender}, reply, node.MsgTransport.Send)
}

func (node *RaftNode) debugLog(format string, args ...interface{}) {
	node.logger.Debug("Node %s: "+format, append([]interface{}{node.serverId}, args...)...)
}

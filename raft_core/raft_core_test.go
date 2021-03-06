package raft_core

import (
	"encoding/json"
	"fmt"
	"github.com/hallliu/golang-raft/transporter"
	"github.com/op/go-logging"
	"os"
	"testing"
	"time"
)

// Makes an isolated Raft node (i.e. one that doesn't time out and has no real peers)
// clusterSize: the size of the cluster that the node thinks that it has
// Node will believe that its own name is "self" and that its peers are "host1", ..., "host(clusterSize-1)"
// Returns the node, the send channel, the recv channel, and the commit channel.
func makeIsolatedNode(clusterSize int) (*RaftNode, chan *transporter.Message, chan *transporter.Message, chan []byte) {
	peerNames := make([]string, clusterSize, clusterSize)
	for i := 0; i < clusterSize-1; i++ {
		peerNames[i] = "host" + string(i+1)
	}
	peerNames[clusterSize-1] = "self"
	sendChan := make(chan *transporter.Message, 10)
	recvChan := make(chan *transporter.Message, 10)
	transporter := transporter.Transporter{sendChan, recvChan}

	commitChan := make(chan []byte, 10)
	node := MakeRaftNode("self", peerNames, transporter, commitChan)
	node.noTimeout = true

	return node, sendChan, recvChan, commitChan
}

func drainChan(c chan *transporter.Message) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}

// Sets up the go-logging stuff
func loggingSetup(level logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(
		"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
	)
	formattedBackend := logging.NewBackendFormatter(backend, format)
	leveledBackend := logging.AddModuleLevel(formattedBackend)
	leveledBackend.SetLevel(level, "raft_core")
	logging.SetBackend(leveledBackend)
}

func unwrap(message *transporter.Message) (cmdType raftCommandType, command interface{}) {
	var wrappedCommand commandWrapper
	json.Unmarshal(message.Command, &wrappedCommand)
	switch wrappedCommand.CommandType {
	case appendEntriesType:
		var cmd appendEntriesCmd
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		return wrappedCommand.CommandType, cmd
	case requestVoteType:
		var cmd requestVoteCmd
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		return wrappedCommand.CommandType, cmd
	case appendEntriesReplyType:
		var cmd appendEntriesReply
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		return wrappedCommand.CommandType, cmd
	case requestVoteReplyType:
		var cmd requestVoteReply
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		return wrappedCommand.CommandType, cmd
	case clientCommandReplyType:
		var cmd clientCommandReply
		json.Unmarshal(wrappedCommand.CommandJson, &cmd)
		return wrappedCommand.CommandType, cmd
	default:
		return appendEntriesReplyType, nil
	}
}

// Tests the different cases under which an appendEntries should be rejected
func TestAppendEntriesRejection(t *testing.T) {
	loggingSetup(logging.DEBUG)
	newNode, send, recv, _ := makeIsolatedNode(5)
	newNode.currentTerm = 3
	go newNode.run()
	cmd := appendEntriesCmd{
		Term:         2,
		LeaderId:     "host1",
		PrevLogIndex: 1000,
		PrevLogTerm:  2,
	}
	sendMessage("host1", []string{"self"}, cmd, recv)
	_, replyInterface := unwrap(<-send)
	reply := replyInterface.(appendEntriesReply)
	if reply.Term != 3 {
		t.Error("Wrong term. Expected 3, got %d", reply.Term)
	}
	if reply.Success {
		t.Error("Got success. Expected failure")
	}
}

// Tests the ability of a node to become a leader after an acceptable number of granted votes
func TestLeaderAscension(t *testing.T) {
	loggingSetup(logging.DEBUG)
	newNode, _, recv, _ := makeIsolatedNode(5)
	newNode.currentTerm = 0
	go newNode.run()
	newNode.beginCandidacy()

	grantedVotecmd := requestVoteReply{
		Term:        1,
		VoteGranted: true,
	}
	deniedVotecmd := requestVoteReply{
		Term:        1,
		VoteGranted: false,
	}
	sendMessage("host1", []string{"self"}, grantedVotecmd, recv)
	sendMessage("host2", []string{"self"}, grantedVotecmd, recv)
	sendMessage("host3", []string{"self"}, grantedVotecmd, recv)
	sendMessage("host4", []string{"self"}, deniedVotecmd, recv)

	time.Sleep(50 * time.Microsecond)

	if newNode.currentRole != clusterLeader {
		t.Error("Failed to become leader.")
	}

}

// Test ability to commit entries after sufficient number of appendEntryReplies
func TestCommittingEntries(t *testing.T) {
	loggingSetup(logging.DEBUG)
	newNode, send, recv, commit := makeIsolatedNode(5)
	newNode.currentTerm = 0
	go newNode.run()
	newNode.becomeLeader()
	sendMessage(transporter.CLIENT, []string{"self"}, clientCommand{
		ClientCommand: []byte("some command json goes here"),
		CommandId:     3,
	}, recv)
	time.Sleep(50 * time.Millisecond)
	drainChan(send)
	newNode.heartBeat()
	for i := 0; i < 4; i++ {
		select {
		case msg := <-send:
			ct, msg1 := unwrap(msg)
			if ct != appendEntriesType {
				t.Error("Node is sending weird shit after a heartbeat")
			}
			sendMessage(msg.Destination, []string{"self"}, appendEntriesReply{
				OriginalMessage: msg1.(appendEntriesCmd),
				Term:            0,
				Success:         true,
			}, recv)
		default:
			t.Error("Not enough appendEntries sent out")
		}
	}
	replyType, reply := unwrap(<-send)
	if replyType != clientCommandReplyType {
		t.Error("Didn't receive reply type")
	}
	if reply.(clientCommandReply).CommandId != 3 {
		t.Error("Wrong command id received: %d", reply.(clientCommandReply).CommandId)
	}
	select {
	case s := <-commit:
		fmt.Println("%+v\n", s)
		if string(s) != "some command json goes here" {
			t.Error("Committed message is corrupted: %s", string(s))

		}
	default:
		t.Error("No commit received")
	}
}

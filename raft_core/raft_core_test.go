package raft_core

import (
	"github.com/hallliu/golang-raft/transporter"
	"github.com/op/go-logging"
	"os"
	"testing"
)

// Makes an isolated Raft node (i.e. one that doesn't time out and has no real peers)
// clusterSize: the size of the cluster that the node thinks that it has
// Node will believe that its own name is "self" and that its peers are "host1", ..., "host(clusterSize-1)"
// Returns the node, the send channel, the recv channel, and the commit channel.
func makeIsolatedNode(clusterSize int) (*RaftNode, chan *transporter.Message, chan *transporter.Message, chan []byte) {
	peerNames := make([]string, len(hostnames), len(hostnames))
	for i := 0; i < clusterSize-1; i++ {
		peerNames[i] = "host" + string(i+1)
	}
	peerNames[clusterSize-1] = "self"
	sendChan := make(chan *transporter.Message)
	recvChan := make(chan *transporter.Message)
	transporter := transporter.Transporter{sendChan, recvChan}

	commitChan := make(chan []byte)
	node := MakeRaftNode("self", peerNames, transporter, commitChan)
	node.noTimeout = true

	return node, sendChan, recvChan, commitChan
}

// Sets up the go-logging stuff
func loggingSetup(level logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	leveledBackend := logging.AddModuleLevel(backend)
	leveledBackend.setLevel(level, "raft_core")
	logging.SetBackend(leveledBackend)
}

func unwrap(message *transporter.Message) (cmdType raftCommandType, command interface{}) {
	var wrappedCommand commandWrapper
	json.Unmarshal(message.Command, wrappedCommand)
	switch wrappedCommand.commandType {
	case appendEntriesType:
		var cmd appendEntriesCmd
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		return wrappedCommand.commandType, cmd
	case requestVoteType:
		var cmd requestVoteCmd
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		return wrappedCommand.commandType, cmd
	case appendEntriesReplyType:
		var cmd appendEntriesReply
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		return wrappedCommand.commandType, cmd
	case requestVoteReplyType:
		var cmd requestVoteReply
		json.Unmarshal(wrappedCommand.commandJson, cmd)
		return wrappedCommand.commandType, cmd
	}
}

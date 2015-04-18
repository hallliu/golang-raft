// Functions that do not involve the raft node directly, but rather act as helpers
// for its actions.

package raft_core

import (
	"encoding/json"
	"fmt"
	"github.com/hallliu/golang-raft/transporter"
	"math/rand"
	"time"
)

var _ = fmt.Errorf

func getElectionTimeout() time.Duration {
	timeoutOffset := rand.Int63n(200)
	return time.Duration(150+timeoutOffset) * time.Millisecond
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

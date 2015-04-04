package raft_core

import (
	"raft/transporter"
	"time"
)

type raftCommandType int

const (
	appendEntriesType raftCommandType = iota
	requestVoteType
	replyType
)

type serverRole int

const (
	clusterLeader serverRole = iota
	leaderCandidate
	clusterFollower
)

type appendEntriesCmd struct {
	commandType  raftCommandType
	term         int
	leaderId     string
	prevLogIndex int
	prevLogTerm  int

	entries           []logEntry
	leaderCommitIndex int
}

type requestVoteCmd struct {
	commandType  raftCommandType
	term         int
	candidateId  string
	lastLogIndex int
	lastLogTerm  int
}

type replyCommand struct {
	commandType raftCommandType
	term        int
	success     bool
}

type logEntry struct {
	term    int
	command []byte
}

type RaftNode struct {
	MsgTransport  transporter.Transporter
	CommitChannel chan []byte
	hostnames     []string

	serverId      string
	currentLeader string
	currentRole   serverRole

	currentTerm int
	votedFor    string
	messageLog  []logEntry

	commitIndex int
	lastApplied int

	nextIndex      map[string]int
	matchIndex     map[string]int
	currentTimeout <-chan time.Time
}

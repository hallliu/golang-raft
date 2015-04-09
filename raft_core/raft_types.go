package raft_core

import (
	"github.com/hallliu/golang-raft/transporter"
	"time"
)

// Constant definitions for the server's role (leader, candidate, or follower)
type serverRole int

const (
	clusterLeader serverRole = iota
	leaderCandidate
	clusterFollower
)

func (r serverRole) String() string {
	switch r {
	case clusterLeader:
		return "Leader"
	case leaderCandidate:
		return "Candidate"
	case clusterFollower:
		return "Follower"
	default:
		return "undefined"
	}
}

// Constant definitions and struct definitions for the various messages that get sent around.
type raftCommandType int

const (
	appendEntriesType raftCommandType = iota
	requestVoteType
	appendEntriesReplyType
	requestVoteReplyType
)

type commandWrapper struct {
	commandType raftCommandType
	commandJson []byte
}

type appendEntriesCmd struct {
	term         int
	leaderId     string
	prevLogIndex int
	prevLogTerm  int

	entries           []logEntry
	leaderCommitIndex int
}

type requestVoteCmd struct {
	term         int
	candidateId  string
	lastLogIndex int
	lastLogTerm  int
}

type appendEntriesReply struct {
	originalMessage appendEntriesCmd
	term            int
	success         bool
}

type requestVoteReply struct {
	term        int
	voteGranted bool
}

// The data that makes up the state of a single Raft node
type logEntry struct {
	term    int
	command []byte
}

type RaftNode struct {
	MsgTransport  transporter.Transporter
	CommitChannel chan []byte
	peernames     []string

	serverId      string
	currentLeader string
	currentRole   serverRole

	currentTerm int
	votedFor    string
	messageLog  []logEntry

	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	numVotes       int
	currentTimeout <-chan time.Time
}

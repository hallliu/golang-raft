package raft_core

import (
	"github.com/hallliu/golang-raft/transporter"
	"github.com/op/go-logging"
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
	clientCommandType
)

type commandWrapper struct {
	CommandType raftCommandType
	CommandJson []byte
}

type appendEntriesCmd struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int

	Entries           []logEntry
	LeaderCommitIndex int
}

type requestVoteCmd struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type appendEntriesReply struct {
	OriginalMessage appendEntriesCmd
	Term            int
	Success         bool
}

type requestVoteReply struct {
	Term        int
	VoteGranted bool
}

type clientCommand struct {
	ClientCommand []byte
}

// The data that makes up the state of a single Raft node
type logEntry struct {
	Term    int
	Command []byte
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

	// Debug data only from here below
	logger    *logging.Logger
	noTimeout bool
}

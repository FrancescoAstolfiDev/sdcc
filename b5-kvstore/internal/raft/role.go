package raft

import "b5-kvstore/pkg/pb"

// Role mirrors pb.Role (consensus.proto) as the node's internal role
// tracking type. A node always boots as Follower regardless of what
// state.json contains (§2).
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (r Role) toProto() pb.Role {
	switch r {
	case Candidate:
		return pb.Role_CANDIDATE
	case Leader:
		return pb.Role_LEADER
	default:
		return pb.Role_FOLLOWER
	}
}

// Package grpcutil holds the one gRPC dialing helper every internal client
// that talks to a peer on the Docker bridge network must go through.
//
// Week 4's manual fault-injection testing found the consensus transport
// (internal/raft/grpctransport) dialing peers with gRPC-Go's default `dns`
// resolver scheme, which caches resolution and does not reliably re-resolve
// a peer's hostname after that peer's container is killed and restarted
// (Docker can assign it a new IP on the bridge network). The fix: dial every
// peer as `passthrough:///<host>:<port>` instead of a bare `<host>:<port>`
// target, so every reconnection attempt (including automatic backoff
// retries) re-resolves via a fresh net.Dial against Docker's embedded DNS
// instead of a cached gRPC resolver.
//
// Week 5 reuses this same fix for the two new gRPC directions introduced by
// the Snapshot & Backup service (internal/snapshot's client toward consensus
// nodes, and internal/raft's new client toward the Snapshot Catalog API) —
// see md-week5.md §0. Any new client dialing a peer inside the cluster
// should call DialPassthrough instead of grpc.NewClient directly.
package grpcutil

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialPassthrough dials addr using gRPC's passthrough scheme. If no dial
// options are given, it defaults to plaintext (insecure) credentials, since
// every caller in this project only ever dials peers inside the isolated
// Docker network (§11.2), never across a public network boundary.
func DialPassthrough(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return grpc.NewClient("passthrough:///"+addr, opts...)
}

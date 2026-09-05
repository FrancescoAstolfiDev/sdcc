package proxy

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/raft/harness"
	"b5-kvstore/internal/statemachine"
	"b5-kvstore/pkg/pb"
)

// This file is a thin adapter: it lets Proxy's routing/redirect/Read-Index
// logic run against real in-process raft.Node instances (via
// internal/raft/harness) instead of real gRPC, at unit-test speed. Every
// node in the cluster gets its own
// statemachine.Server (wired to the harness's ApplyHook), and
// inprocessFactory/harnessDiscovery adapt those servers to the same
// KVClientFactory/DiscoveryClient interfaces the production gRPC-backed
// implementations satisfy.

type inprocessKVClient struct{ server *statemachine.Server }

func (c inprocessKVClient) Get(ctx context.Context, req *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetReply, error) {
	return c.server.Get(ctx, req)
}
func (c inprocessKVClient) Put(ctx context.Context, req *pb.PutRequest, _ ...grpc.CallOption) (*pb.WriteReply, error) {
	return c.server.Put(ctx, req)
}
func (c inprocessKVClient) Update(ctx context.Context, req *pb.PutRequest, _ ...grpc.CallOption) (*pb.WriteReply, error) {
	return c.server.Update(ctx, req)
}
func (c inprocessKVClient) Delete(ctx context.Context, req *pb.DeleteRequest, _ ...grpc.CallOption) (*pb.WriteReply, error) {
	return c.server.Delete(ctx, req)
}

type inprocessFactory struct {
	servers map[string]*statemachine.Server
}

func (f *inprocessFactory) KVClientFor(addr string) pb.KVServiceClient {
	return inprocessKVClient{server: f.servers[addr]}
}

// harnessDiscovery is a DiscoveryClient that inspects the live harness
// cluster on every call — standing in for Service Discovery's periodic
// polling (already covered by internal/discovery's own tests), so this
// test can focus purely on the proxy's behavior against a real cluster.
type harnessDiscovery struct{ cluster *harness.Cluster }

func (h *harnessDiscovery) GetClusterView(context.Context) (*pb.ClusterView, error) {
	view := &pb.ClusterView{}
	var leaderTerm uint64
	haveLeader := false
	for _, id := range h.cluster.IDs {
		role, term := h.cluster.Nodes[id].Status()
		info := &pb.NodeInfo{NodeId: id, Address: id, Role: toProtoRole(role), Term: term}
		view.AllNodes = append(view.AllNodes, info)
		switch role {
		case raft.Leader:
			// A partitioned former leader never steps down on its own, so
			// more than one node can self-report Leader at once — same
			// disambiguation as internal/discovery's real Registry: prefer
			// the highest term.
			if !haveLeader || term >= leaderTerm {
				view.LeaderAddress = id
				leaderTerm = term
				haveLeader = true
			}
		case raft.Follower:
			view.Followers = append(view.Followers, info)
		}
	}
	return view, nil
}

func toProtoRole(r raft.Role) pb.Role {
	switch r {
	case raft.Leader:
		return pb.Role_LEADER
	case raft.Candidate:
		return pb.Role_CANDIDATE
	default:
		return pb.Role_FOLLOWER
	}
}

// newInprocessCluster builds a 3-node harness cluster, one statemachine.KV +
// statemachine.Server per node (wired via WithApplyHook), and waits for a
// leader.
func newInprocessCluster(t *testing.T) (*harness.Cluster, *inprocessFactory) {
	t.Helper()
	kvs := make(map[string]*statemachine.KV)
	servers := make(map[string]*statemachine.Server)

	dirs := map[string]string{}
	cluster, err := harness.NewCluster(3, func(id string) string {
		if dirs[id] == "" {
			dirs[id] = t.TempDir()
		}
		return dirs[id]
	}, harness.FastTiming(), harness.WithApplyHook(func(id string, msg raft.ApplyMsg) {
		var cmd pb.KVCommand
		if err := proto.Unmarshal(msg.Command, &cmd); err != nil {
			t.Errorf("node %s: failed to decode command: %v", id, err)
			return
		}
		kvs[id].Apply(&cmd)
	}))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(cluster.Stop)

	for _, id := range cluster.IDs {
		kv := statemachine.New()
		kvs[id] = kv
		servers[id] = statemachine.NewServer(cluster.Nodes[id], kv)
	}

	if _, _, err := cluster.WaitForLeader(2 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	return cluster, &inprocessFactory{servers: servers}
}

func TestIntegration_WriteThenLinearizableFollowerRead(t *testing.T) {
	cluster, factory := newInprocessCluster(t)
	p := NewProxy(&harnessDiscovery{cluster: cluster}, factory, Config{RPCTimeout: 2 * time.Second})
	p.refreshNow(context.Background())

	if _, aerr := p.Write(context.Background(), OpPut, "k1", "v1"); aerr != nil {
		t.Fatalf("Write: %v", aerr)
	}

	// Re-sync the cache (Write may have discovered a leader change, but
	// here it also lets us pick up the followers list before reading).
	p.refreshNow(context.Background())

	reply, aerr := p.Get(context.Background(), "k1")
	if aerr != nil {
		t.Fatalf("Get: %v", aerr)
	}
	if reply.GetValue() != "v1" {
		t.Fatalf("value = %q, want v1", reply.GetValue())
	}
}

func TestIntegration_LeaderCrashProxyFollowsRedirectAfterRefresh(t *testing.T) {
	cluster, factory := newInprocessCluster(t)
	p := NewProxy(&harnessDiscovery{cluster: cluster}, factory, Config{RPCTimeout: 2 * time.Second})
	p.refreshNow(context.Background())

	if _, aerr := p.Write(context.Background(), OpPut, "k1", "v1"); aerr != nil {
		t.Fatalf("initial Write: %v", aerr)
	}

	oldLeader, oldTerm := p.currentView().GetLeaderAddress(), uint64(0)
	if role, term := cluster.Nodes[oldLeader].Status(); role == raft.Leader {
		oldTerm = term
	}
	cluster.Net.SetFilter(harness.Disconnected(oldLeader))
	t.Cleanup(func() { cluster.Net.SetFilter(nil) })

	// A fully partitioned leader never steps down on its own (correct Raft
	// behavior: only observing a higher term makes it revert), so
	// cluster.WaitForLeader's whole-cluster "exactly one leader" check can
	// never succeed again here — it would see the stale partitioned leader
	// forever. Instead, poll only the two still-connected nodes for a new
	// leader at a higher term.
	var newLeader string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range cluster.IDs {
			if id == oldLeader {
				continue
			}
			role, term := cluster.Nodes[id].Status()
			if role == raft.Leader && term > oldTerm {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatalf("no new leader elected among the connected nodes within the deadline")
	}

	p.refreshNow(context.Background())
	if got := p.currentView().GetLeaderAddress(); got == oldLeader {
		t.Fatalf("cached leader still %q after it was partitioned off", oldLeader)
	}

	if _, aerr := p.Write(context.Background(), OpPut, "k2", "v2"); aerr != nil {
		t.Fatalf("Write against new leader: %v", aerr)
	}
}

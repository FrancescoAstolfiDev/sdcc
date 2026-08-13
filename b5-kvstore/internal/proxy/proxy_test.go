package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	"b5-kvstore/pkg/pb"
)

func newTestProxy(t *testing.T, view *pb.ClusterView, factory *fakeFactory) *Proxy {
	t.Helper()
	// A generous timeout: fake calls return instantly, this just needs to
	// never actually elapse during a test.
	p := NewProxy(&fakeDiscovery{view: view}, factory, Config{RPCTimeout: 5 * time.Second})
	p.refreshNow(context.Background())
	return p
}

func viewWith(leader string, followers ...string) *pb.ClusterView {
	v := &pb.ClusterView{LeaderAddress: leader}
	all := []*pb.NodeInfo{{Address: leader, Role: pb.Role_LEADER}}
	for _, f := range followers {
		info := &pb.NodeInfo{Address: f, Role: pb.Role_FOLLOWER}
		v.Followers = append(v.Followers, info)
		all = append(all, info)
	}
	v.AllNodes = all
	return v
}

// --- Write: §3 generic redirect-following -----------------------------

func TestWrite_DirectSuccessToLeader(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		putFn: func(_ context.Context, req *pb.PutRequest) (*pb.WriteReply, error) {
			return &pb.WriteReply{Success: true, CommitIndex: 7}, nil
		},
	}
	p := newTestProxy(t, viewWith("leader:1"), factory)

	reply, aerr := p.Write(context.Background(), OpPut, "k", "v")
	if aerr != nil {
		t.Fatalf("unexpected error: %v", aerr)
	}
	if reply.GetCommitIndex() != 7 {
		t.Fatalf("commitIndex = %d, want 7", reply.GetCommitIndex())
	}
}

func TestWrite_FollowsSingleRedirectThenSucceeds(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["stale-leader:1"] = &fakeKVClient{
		putFn: func(_ context.Context, req *pb.PutRequest) (*pb.WriteReply, error) {
			return &pb.WriteReply{Success: false, RedirectLeader: "real-leader:2"}, nil
		},
	}
	factory.clients["real-leader:2"] = &fakeKVClient{
		putFn: func(_ context.Context, req *pb.PutRequest) (*pb.WriteReply, error) {
			return &pb.WriteReply{Success: true, CommitIndex: 3}, nil
		},
	}
	p := newTestProxy(t, viewWith("stale-leader:1"), factory)

	reply, aerr := p.Write(context.Background(), OpPut, "k", "v")
	if aerr != nil {
		t.Fatalf("unexpected error: %v", aerr)
	}
	if reply.GetCommitIndex() != 3 {
		t.Fatalf("commitIndex = %d, want 3", reply.GetCommitIndex())
	}
	if got := p.currentView().GetLeaderAddress(); got != "real-leader:2" {
		t.Fatalf("cached leader after redirect = %q, want real-leader:2 (cache must be invalidated on redirect)", got)
	}
}

func TestWrite_HopBudgetExhaustedReturns503(t *testing.T) {
	factory := newFakeFactory()
	// Every node redirects to the next, in a cycle — redirect-following
	// must give up after maxHops (3), not loop forever.
	factory.clients["a"] = &fakeKVClient{putFn: redirectTo("b")}
	factory.clients["b"] = &fakeKVClient{putFn: redirectTo("c")}
	factory.clients["c"] = &fakeKVClient{putFn: redirectTo("a")}
	p := newTestProxy(t, viewWith("a"), factory)

	_, aerr := p.Write(context.Background(), OpPut, "k", "v")
	if aerr == nil || aerr.status != 503 || aerr.code != "unavailable" {
		t.Fatalf("expected 503 unavailable, got %+v", aerr)
	}
	if len(factory.calls) != maxHops {
		t.Fatalf("expected exactly %d attempts, got %d (%v)", maxHops, len(factory.calls), factory.calls)
	}
}

func redirectTo(addr string) func(context.Context, *pb.PutRequest) (*pb.WriteReply, error) {
	return func(context.Context, *pb.PutRequest) (*pb.WriteReply, error) {
		return &pb.WriteReply{Success: false, RedirectLeader: addr}, nil
	}
}

func TestWrite_TransportErrorReturns503(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		putFn: func(context.Context, *pb.PutRequest) (*pb.WriteReply, error) {
			return nil, errors.New("connection refused")
		},
	}
	p := newTestProxy(t, viewWith("leader:1"), factory)

	_, aerr := p.Write(context.Background(), OpPut, "k", "v")
	if aerr == nil || aerr.status != 503 {
		t.Fatalf("expected 503, got %+v", aerr)
	}
}

func TestWrite_NoKnownLeaderReturns503(t *testing.T) {
	p := newTestProxy(t, &pb.ClusterView{}, newFakeFactory())
	_, aerr := p.Write(context.Background(), OpPut, "k", "v")
	if aerr == nil || aerr.status != 503 {
		t.Fatalf("expected 503, got %+v", aerr)
	}
}

// --- Get: round-robin follower via Read-Index, vs. §4's single-hop -----
// fallback, vs. §3's generic redirect-following on the leader-default path.

func TestGet_NoFollowersRoutesDirectlyToLeader(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: true, Value: "v1"}, nil
		},
	}
	p := newTestProxy(t, viewWith("leader:1"), factory)

	reply, aerr := p.Get(context.Background(), "k")
	if aerr != nil {
		t.Fatalf("unexpected error: %v", aerr)
	}
	if reply.GetValue() != "v1" {
		t.Fatalf("value = %q, want v1", reply.GetValue())
	}
}

func TestGet_RoundRobinsAcrossFollowersAndUsesReadIndexResult(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["f1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: true, Value: "from-f1"}, nil
		},
	}
	factory.clients["f2"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: true, Value: "from-f2"}, nil
		},
	}
	p := newTestProxy(t, viewWith("leader:1", "f1", "f2"), factory)

	var values []string
	for i := 0; i < 4; i++ {
		reply, aerr := p.Get(context.Background(), "k")
		if aerr != nil {
			t.Fatalf("unexpected error: %v", aerr)
		}
		values = append(values, reply.GetValue())
	}
	want := []string{"from-f1", "from-f2", "from-f1", "from-f2"}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("round-robin sequence = %v, want %v", values, want)
		}
	}
}

func TestGet_FollowerReadIndexNotOk_FallsBackSingleHopToLeader(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["f1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: false, RedirectLeader: "leader:1"}, nil
		},
	}
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: true, Value: "authoritative"}, nil
		},
	}
	p := newTestProxy(t, viewWith("leader:1", "f1"), factory)

	reply, aerr := p.Get(context.Background(), "k")
	if aerr != nil {
		t.Fatalf("unexpected error: %v", aerr)
	}
	if reply.GetValue() != "authoritative" {
		t.Fatalf("value = %q, want authoritative", reply.GetValue())
	}
	// Exactly f1 then leader:1 — no second follower attempted (§4).
	if len(factory.calls) != 2 || factory.calls[0] != "f1" || factory.calls[1] != "leader:1" {
		t.Fatalf("calls = %v, want [f1 leader:1]", factory.calls)
	}
}

func TestGet_FollowerTransportErrorFallsBackSingleHopToLeader_NotASecondFollower(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["f1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return nil, errors.New("dial timeout")
		},
	}
	factory.clients["f2"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			t.Fatal("must never attempt a second follower per §4's runtime fallback rule")
			return nil, nil
		},
	}
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: true, Value: "authoritative"}, nil
		},
	}
	// Force round-robin to land on f1 first.
	p := newTestProxy(t, viewWith("leader:1", "f1", "f2"), factory)

	reply, aerr := p.Get(context.Background(), "k")
	if aerr != nil {
		t.Fatalf("unexpected error: %v", aerr)
	}
	if reply.GetValue() != "authoritative" {
		t.Fatalf("value = %q, want authoritative", reply.GetValue())
	}
}

func TestGet_KeyNotFoundReturns404(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: false}, nil
		},
	}
	p := newTestProxy(t, viewWith("leader:1"), factory)

	_, aerr := p.Get(context.Background(), "missing")
	if aerr == nil || aerr.status != 404 || aerr.code != "not_found" {
		t.Fatalf("expected 404 not_found, got %+v", aerr)
	}
}

func TestGet_SingleHopFallbackAlsoFailing_Returns503(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["f1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: false}, nil
		},
	}
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return nil, errors.New("leader unreachable too")
		},
	}
	p := newTestProxy(t, viewWith("leader:1", "f1"), factory)

	_, aerr := p.Get(context.Background(), "k")
	if aerr == nil || aerr.status != 503 {
		t.Fatalf("expected 503, got %+v", aerr)
	}
}

// --- Circuit breaker integration: all-open short-circuit ----------------

func TestWrite_AllBreakersOpenReturns503WithoutCallingFactory(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		putFn: func(context.Context, *pb.PutRequest) (*pb.WriteReply, error) {
			return nil, errors.New("boom")
		},
	}
	p := newTestProxy(t, viewWith("leader:1"), factory)

	// Trip the only known node's breaker (3 consecutive failures).
	for i := 0; i < 3; i++ {
		_, _ = p.Write(context.Background(), OpPut, "k", "v")
	}
	factory.calls = nil

	_, aerr := p.Write(context.Background(), OpPut, "k", "v")
	if aerr == nil || aerr.status != 503 {
		t.Fatalf("expected 503, got %+v", aerr)
	}
	if len(factory.calls) != 0 {
		t.Fatalf("expected the all-open short-circuit to skip calling the factory entirely, got calls=%v", factory.calls)
	}
}

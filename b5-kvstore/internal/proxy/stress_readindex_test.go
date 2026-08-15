package proxy

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/raft/harness"
	"b5-kvstore/internal/statemachine"
	"b5-kvstore/pkg/pb"
	"google.golang.org/protobuf/proto"
)

func newInprocessClusterN(t *testing.T, n int) (*harness.Cluster, *inprocessFactory) {
	t.Helper()
	kvs := make(map[string]*statemachine.KV)
	servers := make(map[string]*statemachine.Server)

	dirs := map[string]string{}
	cluster, err := harness.NewCluster(n, func(id string) string {
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

// TestStress_ConcurrentWriteThenOwnKeyRead mimics cmd/loadgen's worker
// pattern (each worker writes its own key then reads it back repeatedly,
// only ever reading a key it itself successfully wrote) but concurrently
// across many workers against a real 5-node in-process cluster, to check
// whether the proxy/Read-Index path can ever spuriously 404 on a key its
// own writer already got a success reply for.
func TestStress_ConcurrentWriteThenOwnKeyRead(t *testing.T) {
	cluster, factory := newInprocessClusterN(t, 5)
	p := NewProxy(&harnessDiscovery{cluster: cluster}, factory, Config{RPCTimeout: 2 * time.Second})
	go p.Start(context.Background(), 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	const workers = 20
	const itersPerWorker = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	var notFoundCount int
	var errs []string

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itersPerWorker; i++ {
				key := fmt.Sprintf("stress-%d-%d", id, i)
				if _, aerr := p.Write(context.Background(), OpPut, key, "v"); aerr != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("write %s: %v", key, aerr))
					mu.Unlock()
					continue
				}
				// read own key back several times immediately, like loadgen does across the run
				for r := 0; r < 3; r++ {
					reply, aerr := p.Get(context.Background(), key)
					if aerr != nil {
						mu.Lock()
						notFoundCount++
						errs = append(errs, fmt.Sprintf("read %s attempt %d: %v", key, r, aerr))
						mu.Unlock()
						continue
					}
					if reply.GetValue() != "v" {
						mu.Lock()
						errs = append(errs, fmt.Sprintf("read %s attempt %d: got value %q", key, r, reply.GetValue()))
						mu.Unlock()
					}
				}
			}
		}(w)
	}
	wg.Wait()

	if notFoundCount > 0 {
		t.Errorf("got %d spurious not-found/error reads out of %d own-key reads", notFoundCount, workers*itersPerWorker*3)
		for i, e := range errs {
			if i > 20 {
				t.Logf("... and %d more", len(errs)-20)
				break
			}
			t.Logf("%s", e)
		}
	}
}

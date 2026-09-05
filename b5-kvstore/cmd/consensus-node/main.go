// Command consensus-node is a single Raft-lite cluster member (spec §3).
// All consensus nodes run this same binary; role (Leader/Follower/Candidate)
// is runtime state, not a build-time distinction.
//
// Wires the internal/raft core to the real pkg/pb gRPC bindings (§6 Step C)
// — RequestVote/AppendEntries/GetStatus/InstallSnapshot are served for real.
// KVService is intentionally not registered here: it's the Client Proxy's
// concern, an explicit non-goal of this binary.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/raft/grpctransport"
	"b5-kvstore/internal/raft/snapshotcatalogclient"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/internal/statemachine"
	"b5-kvstore/pkg/pb"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("consensus-node: NODE_ID is required")
	}
	port := os.Getenv("CONSENSUS_PORT")
	if port == "" {
		port = "9000"
	}
	dataDirBase := os.Getenv("DATA_DIR")
	if dataDirBase == "" {
		dataDirBase = "/data"
	}
	// Independent directory per node (§1), even though each container's
	// /data is already a dedicated Docker volume — this also makes the
	// same binary safe to run several times against a shared base path in
	// non-Docker local testing.
	dataDir := filepath.Join(dataDirBase, nodeID)

	// PEERS is one shared static list mounted identically into every
	// consensus-node container (§10.6/§11: never resolved via Service
	// Discovery). Each node filters itself out by matching its own
	// CONSENSUS_PORT against the port in each "host:port" entry.
	peersEnv := os.Getenv("PEERS")
	peers := peerAddresses(peersEnv, port)

	// The node's own advertised address, resolved from the same PEERS list
	// (single source of truth) by matching our own port — needed now that
	// the Read-Index handshake and redirect_leader responses require a
	// dialable "host:port", not just ":port".
	address := ownAddress(peersEnv, port)
	if address == "" {
		log.Printf("consensus-node[%s]: WARNING: own entry not found in PEERS=%q; falling back to :%s, which peers cannot dial", nodeID, peersEnv, port)
		address = net.JoinHostPort("", port)
	}

	timing, err := raft.LoadTimingConfigFromEnv()
	if err != nil {
		log.Fatalf("consensus-node[%s]: %v", nodeID, err)
	}
	snapshotCfg, err := raft.LoadSnapshotConfigFromEnv()
	if err != nil {
		log.Fatalf("consensus-node[%s]: %v", nodeID, err)
	}

	transport := grpctransport.New()
	defer transport.Close()

	kv := statemachine.New()

	// §3.6's startup sequence: seed lastApplied and the state machine from
	// the latest local snapshot (if any) before anything else touches kv or
	// constructs the Node.
	var localSnapshot *snapshotfile.File
	if file, _, found, err := snapshotfile.Latest(dataDir); err != nil {
		log.Fatalf("consensus-node[%s]: failed to load local snapshot: %v", nodeID, err)
	} else if found {
		kv.Restore(file.State)
		localSnapshot = &file
	}

	snapshotAddr := os.Getenv("SNAPSHOT_ADDR")
	if snapshotAddr == "" {
		log.Fatalf("consensus-node[%s]: SNAPSHOT_ADDR is required", nodeID)
	}
	snapshotCatalog, err := snapshotcatalogclient.New(snapshotAddr)
	if err != nil {
		log.Fatalf("consensus-node[%s]: failed to dial snapshot-backup at %s: %v", nodeID, snapshotAddr, err)
	}
	defer snapshotCatalog.Close()

	node, err := raft.NewNode(raft.Config{
		ID:              nodeID,
		Address:         address,
		DataDir:         dataDir,
		Peers:           peers,
		Timing:          timing,
		Transport:       transport,
		Snapshot:        snapshotCfg,
		SnapshotCatalog: snapshotCatalog,
		StateMachine:    kv,
		LocalSnapshot:   localSnapshot,
		ApplyFn: func(msg raft.ApplyMsg) {
			var cmd pb.KVCommand
			if err := proto.Unmarshal(msg.Command, &cmd); err != nil {
				log.Printf("consensus-node[%s]: failed to decode command at index=%d: %v", nodeID, msg.Index, err)
				return
			}
			kv.Apply(&cmd)
			log.Printf("consensus-node[%s]: applied index=%d term=%d op=%s key=%q", nodeID, msg.Index, msg.Term, cmd.GetOp(), cmd.GetKey())
		},
	})
	if err != nil {
		log.Fatalf("consensus-node[%s]: failed to initialize raft node: %v", nodeID, err)
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("consensus-node[%s]: failed to listen on :%s: %v", nodeID, port, err)
	}

	srv := grpc.NewServer()
	pb.RegisterConsensusServer(srv, node)
	pb.RegisterKVServiceServer(srv, statemachine.NewServer(node, kv))
	pb.RegisterSnapshotTransferServer(srv, node)

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	node.Start()
	defer node.Stop()

	go func() {
		log.Printf("consensus-node[%s]: listening on :%s (peers=%v, discovery=%s)",
			nodeID, port, peers, os.Getenv("DISCOVERY_ADDR"))
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("consensus-node[%s]: serve error: %v", nodeID, err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Printf("consensus-node[%s]: shutting down", nodeID)
	srv.GracefulStop()
}

// peerAddresses parses a comma-separated "host:port" PEERS list and returns
// every entry except the one whose port matches ownPort (i.e. this node's
// own entry in the shared static list).
func peerAddresses(peersEnv, ownPort string) []string {
	var out []string
	for _, raw := range strings.Split(peersEnv, ",") {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			continue
		}
		_, p, err := net.SplitHostPort(addr)
		if err == nil && p == ownPort {
			continue // this is our own entry in the shared list
		}
		out = append(out, addr)
	}
	return out
}

// ownAddress is the mirror image of peerAddresses: it returns the one entry
// in the shared PEERS list whose port matches ownPort (i.e. this node's own
// dialable "host:port"), or "" if no entry matches.
func ownAddress(peersEnv, ownPort string) string {
	for _, raw := range strings.Split(peersEnv, ",") {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			continue
		}
		_, p, err := net.SplitHostPort(addr)
		if err == nil && p == ownPort {
			return addr
		}
	}
	return ""
}

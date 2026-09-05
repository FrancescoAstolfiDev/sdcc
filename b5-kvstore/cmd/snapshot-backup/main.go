// Command snapshot-backup is the asynchronous Snapshot & Backup service
// (spec §5): polls the current leader's log occupancy and compacts it into
// stable checkpoint files, and serves the Snapshot Catalog API (§9.5) so
// every consensus node can independently catch up (§5.3), wiring the
// polling/compaction cycle (internal/snapshot.Compactor) and the
// SnapshotCatalog server together.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"b5-kvstore/internal/snapshot"
	"b5-kvstore/pkg/pb"
)

func main() {
	port := os.Getenv("SNAPSHOT_PORT")
	if port == "" {
		port = "8600"
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}
	discoveryAddr := os.Getenv("DISCOVERY_ADDR")
	if discoveryAddr == "" {
		log.Fatal("snapshot-backup: DISCOVERY_ADDR is required")
	}

	cfg, err := snapshot.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("snapshot-backup: %v", err)
	}

	store, err := snapshot.NewStore(dataDir)
	if err != nil {
		log.Fatalf("snapshot-backup: failed to load local snapshot state: %v", err)
	}

	discoveryClient, err := snapshot.NewGRPCDiscoveryClient(discoveryAddr)
	if err != nil {
		log.Fatalf("snapshot-backup: failed to dial discovery at %s: %v", discoveryAddr, err)
	}
	defer discoveryClient.Close()

	transferClient := snapshot.NewGRPCTransferClient()
	defer transferClient.Close()

	compactor := snapshot.NewCompactor(discoveryClient, transferClient, store, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go compactor.Run(ctx)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("snapshot-backup: failed to listen on :%s: %v", port, err)
	}

	srv := grpc.NewServer()
	pb.RegisterSnapshotCatalogServer(srv, snapshot.NewCatalogServer(store))

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	go func() {
		log.Printf("snapshot-backup: listening on :%s (discovery=%s)", port, discoveryAddr)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("snapshot-backup: serve error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("snapshot-backup: shutting down")
	srv.GracefulStop()
}

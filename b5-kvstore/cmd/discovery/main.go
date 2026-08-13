// Command discovery is the Service Discovery node (spec §7, md-week4 §1): a
// pull-based polling registry over the static peer list, queried by the
// Client Proxy and, later, by the Snapshot & Backup service (Week 5).
// Discovery never sits on the KV data path and never registers nodes
// dynamically — it only polls GetStatus() on a fixed interval.
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

	"b5-kvstore/internal/discovery"
	"b5-kvstore/pkg/pb"
)

func main() {
	port := os.Getenv("DISCOVERY_PORT")
	if port == "" {
		port = "8500"
	}

	cfg, err := discovery.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("discovery: %v", err)
	}

	prober := discovery.NewGRPCProber()
	defer prober.Close()

	reg := discovery.NewRegistry(cfg.Peers, prober, cfg.RPCTimeout)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go reg.Run(ctx, cfg.PollInterval)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("discovery: failed to listen on :%s: %v", port, err)
	}

	srv := grpc.NewServer()
	pb.RegisterDiscoveryServer(srv, discovery.NewServer(reg))

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	go func() {
		log.Printf("discovery: listening on :%s (peers=%v, poll_interval=%s)", port, cfg.Peers, cfg.PollInterval)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("discovery: serve error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("discovery: shutting down")
	srv.GracefulStop()
}

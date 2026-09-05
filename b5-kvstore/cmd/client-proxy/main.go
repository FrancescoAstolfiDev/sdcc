// Command client-proxy is the stateless Client Proxy service (spec §4): the
// only component reachable from the external Client, over REST/JSON, and
// the only component that talks gRPC to the consensus cluster. It owns the
// Circuit Breaker pattern (spec §8) and consults Service Discovery
// (spec §7) for the current cluster map — see internal/proxy for the
// routing/redirect/Read-Index logic itself.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"b5-kvstore/internal/proxy"
)

func main() {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}
	discoveryAddr := os.Getenv("DISCOVERY_ADDR")
	if discoveryAddr == "" {
		log.Fatal("client-proxy: DISCOVERY_ADDR is required")
	}

	cfg, err := proxy.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("client-proxy: %v", err)
	}

	discClient, err := proxy.NewGRPCDiscoveryClient(discoveryAddr)
	if err != nil {
		log.Fatalf("client-proxy: failed to dial discovery at %s: %v", discoveryAddr, err)
	}
	defer discClient.Close()

	factory := proxy.NewGRPCClientFactory()
	defer factory.Close()

	p := proxy.NewProxy(discClient, factory, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go p.Start(ctx, cfg.RefreshInterval)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: proxy.NewHTTPHandler(p),
	}

	go func() {
		log.Printf("client-proxy: listening on :%s (discovery=%s)", port, discoveryAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("client-proxy: serve error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("client-proxy: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

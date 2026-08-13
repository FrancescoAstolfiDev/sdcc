package discovery

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/pkg/pb"
)

// Server implements pb.DiscoveryServer (spec §9.3) over a Registry.
type Server struct {
	pb.UnimplementedDiscoveryServer

	reg *Registry
}

// NewServer wraps reg as a pb.DiscoveryServer.
func NewServer(reg *Registry) *Server {
	return &Server{reg: reg}
}

func (s *Server) GetClusterView(_ context.Context, _ *emptypb.Empty) (*pb.ClusterView, error) {
	return s.reg.ClusterView(), nil
}

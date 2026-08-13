package proxy

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"b5-kvstore/pkg/pb"
)

// fakeKVClient is a pb.KVServiceClient entirely scripted by the test: each
// method delegates to an optional func field, defaulting to "not
// implemented" so a test only needs to set the methods it actually
// exercises. This is the "mock the KVService client" harness §6 asks for
// REST<->gRPC translation tests to use.
type fakeKVClient struct {
	getFn    func(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error)
	putFn    func(ctx context.Context, req *pb.PutRequest) (*pb.WriteReply, error)
	updateFn func(ctx context.Context, req *pb.PutRequest) (*pb.WriteReply, error)
	deleteFn func(ctx context.Context, req *pb.DeleteRequest) (*pb.WriteReply, error)
}

func (f *fakeKVClient) Get(ctx context.Context, req *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetReply, error) {
	if f.getFn == nil {
		return nil, errors.New("fakeKVClient: Get not configured")
	}
	return f.getFn(ctx, req)
}

func (f *fakeKVClient) Put(ctx context.Context, req *pb.PutRequest, _ ...grpc.CallOption) (*pb.WriteReply, error) {
	if f.putFn == nil {
		return nil, errors.New("fakeKVClient: Put not configured")
	}
	return f.putFn(ctx, req)
}

func (f *fakeKVClient) Update(ctx context.Context, req *pb.PutRequest, _ ...grpc.CallOption) (*pb.WriteReply, error) {
	if f.updateFn == nil {
		return nil, errors.New("fakeKVClient: Update not configured")
	}
	return f.updateFn(ctx, req)
}

func (f *fakeKVClient) Delete(ctx context.Context, req *pb.DeleteRequest, _ ...grpc.CallOption) (*pb.WriteReply, error) {
	if f.deleteFn == nil {
		return nil, errors.New("fakeKVClient: Delete not configured")
	}
	return f.deleteFn(ctx, req)
}

// fakeFactory is a KVClientFactory over a fixed address->client map, with
// an optional default for addresses not explicitly listed (used to model
// "this address unexpectedly got called").
type fakeFactory struct {
	clients map[string]*fakeKVClient
	calls   []string // records every address KVClientFor was called with, in order
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{clients: make(map[string]*fakeKVClient)}
}

func (f *fakeFactory) KVClientFor(addr string) pb.KVServiceClient {
	f.calls = append(f.calls, addr)
	if c, ok := f.clients[addr]; ok {
		return c
	}
	return &fakeKVClient{} // every method errors "not configured"
}

// fakeDiscovery is a DiscoveryClient returning a fixed, test-controlled
// ClusterView.
type fakeDiscovery struct {
	view *pb.ClusterView
	err  error
}

func (f *fakeDiscovery) GetClusterView(context.Context) (*pb.ClusterView, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.view, nil
}

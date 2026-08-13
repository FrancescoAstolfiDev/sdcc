// Package statemachine implements the KV map + apply logic that lives
// behind every consensus node (spec §3.1's stateMachine field): the actual
// key-value data, updated only through committed Raft log entries. It has
// no networking or consensus dependencies of its own — KV is a plain,
// mutex-guarded map, unit-testable in isolation, matching the same
// build-order principle already applied to internal/raft/persistence.
package statemachine

import (
	"sync"

	"b5-kvstore/pkg/pb"
)

// KV is the replicated key-value store. Every mutation must come from a
// committed log entry via Apply — nothing here talks to Raft directly, so
// callers (the raft apply loop, via Server in server.go) are responsible
// for only ever calling Apply on entries that have actually committed.
type KV struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty KV store.
func New() *KV {
	return &KV{data: make(map[string]string)}
}

// Get returns the current value for key and whether it is present.
func (kv *KV) Get(key string) (value string, found bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, found = kv.data[key]
	return value, found
}

// Snapshot returns a copy of the entire current state, for the Snapshot &
// Backup service (internal/snapshot) to embed in a compaction checkpoint
// (spec §5.2). A copy, not the live map, so the caller can serialize it
// without holding kv's lock for the duration.
func (kv *KV) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	out := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		out[k] = v
	}
	return out
}

// Restore replaces the entire current state wholesale with data, discarding
// whatever was there before. Used by a node's snapshot catch-up path
// (internal/raft, spec §5.3) to adopt a fetched snapshot as its new
// state-machine baseline.
func (kv *KV) Restore(data map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cp := make(map[string]string, len(data))
	for k, v := range data {
		cp[k] = v
	}
	kv.data = cp
}

// Apply applies a single committed KVCommand (Put/Update/Delete) to the
// map. Put and Update share identical apply semantics (both overwrite the
// key) — the distinction between them only matters at the REST/gRPC layer
// (push vs. update, POST vs. PUT), not to the state machine itself.
func (kv *KV) Apply(cmd *pb.KVCommand) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch cmd.GetOp() {
	case pb.KVCommand_PUT, pb.KVCommand_UPDATE:
		kv.data[cmd.GetKey()] = cmd.GetValue()
	case pb.KVCommand_DELETE:
		delete(kv.data, cmd.GetKey())
	}
}

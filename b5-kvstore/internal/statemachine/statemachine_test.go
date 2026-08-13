package statemachine

import (
	"testing"

	"b5-kvstore/pkg/pb"
)

func TestApplyPutGetDelete(t *testing.T) {
	kv := New()

	if _, found := kv.Get("k"); found {
		t.Fatal("expected key absent before any Apply")
	}

	kv.Apply(&pb.KVCommand{Op: pb.KVCommand_PUT, Key: "k", Value: "v1"})
	if v, found := kv.Get("k"); !found || v != "v1" {
		t.Fatalf("got (%q, %v), want (v1, true)", v, found)
	}

	kv.Apply(&pb.KVCommand{Op: pb.KVCommand_UPDATE, Key: "k", Value: "v2"})
	if v, found := kv.Get("k"); !found || v != "v2" {
		t.Fatalf("got (%q, %v), want (v2, true)", v, found)
	}

	kv.Apply(&pb.KVCommand{Op: pb.KVCommand_DELETE, Key: "k"})
	if _, found := kv.Get("k"); found {
		t.Fatal("expected key absent after DELETE")
	}
}

func TestApplyDeleteIsIdempotent(t *testing.T) {
	kv := New()
	kv.Apply(&pb.KVCommand{Op: pb.KVCommand_DELETE, Key: "missing"})
	if _, found := kv.Get("missing"); found {
		t.Fatal("expected key absent")
	}
}

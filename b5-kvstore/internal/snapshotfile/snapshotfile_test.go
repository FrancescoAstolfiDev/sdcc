package snapshotfile

import (
	"bytes"
	"reflect"
	"testing"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	want := File{
		State:             map[string]string{"a": "1", "b": "2"},
		LastIncludedIndex: 42,
		LastIncludedTerm:  3,
	}
	var buf bytes.Buffer
	if err := Encode(&buf, want); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestWriteReadFile_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := File{
		State:             map[string]string{"k": "v"},
		LastIncludedIndex: 100,
		LastIncludedTerm:  7,
	}
	path, err := WriteFile(dir, want)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := FileName(100); path[len(path)-len(got):] != got {
		t.Fatalf("path %q does not end with expected name %q", path, got)
	}
	got, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestLatest_NoFilesYet(t *testing.T) {
	dir := t.TempDir()
	_, _, found, err := Latest(dir)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if found {
		t.Fatalf("expected found=false on an empty dir")
	}
}

func TestLatest_PicksHighestIndex(t *testing.T) {
	dir := t.TempDir()
	for _, idx := range []uint64{10, 50, 30} {
		if _, err := WriteFile(dir, File{State: map[string]string{}, LastIncludedIndex: idx, LastIncludedTerm: 1}); err != nil {
			t.Fatalf("WriteFile(%d): %v", idx, err)
		}
	}
	file, path, found, err := Latest(dir)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !found {
		t.Fatalf("expected found=true")
	}
	if file.LastIncludedIndex != 50 {
		t.Fatalf("got lastIncludedIndex=%d, want 50", file.LastIncludedIndex)
	}
	want := FileName(50)
	if path[len(path)-len(want):] != want {
		t.Fatalf("path %q does not end with expected name %q", path, want)
	}
}

func TestLatest_MissingDirIsNotAnError(t *testing.T) {
	_, _, found, err := Latest("/nonexistent/path/for/sure")
	if err != nil {
		t.Fatalf("Latest on a missing dir should not error, got: %v", err)
	}
	if found {
		t.Fatalf("expected found=false")
	}
}

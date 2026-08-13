package persistence

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadState_MissingFileReturnsZeroValues(t *testing.T) {
	dir := t.TempDir()
	term, votedFor, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState on missing file: %v", err)
	}
	if term != 0 || votedFor != "" {
		t.Fatalf("expected zero values, got term=%d votedFor=%q", term, votedFor)
	}
}

func TestSaveState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := SaveState(dir, 7, "node-2"); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	term, votedFor, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 7 || votedFor != "node-2" {
		t.Fatalf("got term=%d votedFor=%q, want term=7 votedFor=node-2", term, votedFor)
	}

	// Overwrite must fully replace, not merge.
	if err := SaveState(dir, 9, ""); err != nil {
		t.Fatalf("SaveState overwrite: %v", err)
	}
	term, votedFor, err = LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState after overwrite: %v", err)
	}
	if term != 9 || votedFor != "" {
		t.Fatalf("got term=%d votedFor=%q, want term=9 votedFor=empty", term, votedFor)
	}

	// No leftover temp file.
	if _, err := os.Stat(filepath.Join(dir, stateFileName+".tmp")); !os.IsNotExist(err) {
		t.Fatalf("expected temp state file to be gone, stat err=%v", err)
	}
}

func TestAppendAndReadAllLogEntries_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("put a=1")},
		{Term: 1, Index: 2, Command: []byte("put b=2")},
		{Term: 2, Index: 3, Command: []byte("delete a")},
	}
	if err := AppendLogEntries(dir, want[:2]); err != nil {
		t.Fatalf("AppendLogEntries (batch 1): %v", err)
	}
	if err := AppendLogEntries(dir, want[2:]); err != nil {
		t.Fatalf("AppendLogEntries (batch 2): %v", err)
	}

	got, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestReadAllLogEntries_EmptyLogIsEmptySlice(t *testing.T) {
	dir := t.TempDir()
	got, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries on empty dir: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no entries, got %+v", got)
	}
}

func TestTruncateLogFrom_DropsConflictingEntries(t *testing.T) {
	dir := t.TempDir()
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}
	if err := AppendLogEntries(dir, entries); err != nil {
		t.Fatalf("AppendLogEntries: %v", err)
	}

	if err := TruncateLogFrom(dir, 3); err != nil {
		t.Fatalf("TruncateLogFrom: %v", err)
	}

	got, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries after truncate: %v", err)
	}
	want := entries[:2]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// Re-appending after truncation must produce a clean, readable log
	// (this is exactly how the leader's log-repair path uses it).
	repaired := []LogEntry{{Term: 3, Index: 3, Command: []byte("c-repaired")}}
	if err := AppendLogEntries(dir, repaired); err != nil {
		t.Fatalf("AppendLogEntries after truncate: %v", err)
	}
	got, err = ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries after repair append: %v", err)
	}
	want = append(append([]LogEntry{}, entries[:2]...), repaired...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// TestReadAllLogEntries_RecoversFromTornWrite simulates a crash mid-append:
// the process dies after writing the length prefix (or part of the payload)
// of the last record but before it (and its fsync) completed. Loading the
// log must recover the complete leading entries and physically truncate the
// file, rather than erroring out or returning corrupt data.
func TestReadAllLogEntries_RecoversFromTornWrite(t *testing.T) {
	dir := t.TempDir()
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("first")},
		{Term: 1, Index: 2, Command: []byte("second")},
		{Term: 2, Index: 3, Command: []byte("third-will-be-torn")},
	}
	if err := AppendLogEntries(dir, entries); err != nil {
		t.Fatalf("AppendLogEntries: %v", err)
	}

	fullInfo, err := os.Stat(logPath(dir))
	if err != nil {
		t.Fatalf("stat log.dat: %v", err)
	}
	fullSize := fullInfo.Size()

	// Truncate the file to simulate the crash landing mid-write of the
	// last record (cut off the last 5 bytes, guaranteed to land inside
	// the third record's length-prefix+payload given non-trivial content).
	if err := os.Truncate(logPath(dir), fullSize-5); err != nil {
		t.Fatalf("simulate torn write: %v", err)
	}

	got, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries after torn write: %v", err)
	}
	want := entries[:2]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v (torn trailing record should be dropped)", got, want)
	}

	// The file itself must now be physically truncated, so a second load
	// (and any subsequent append) doesn't see or re-detect the torn tail.
	repairedInfo, err := os.Stat(logPath(dir))
	if err != nil {
		t.Fatalf("stat log.dat after recovery: %v", err)
	}
	if repairedInfo.Size() >= fullSize {
		t.Fatalf("expected log.dat to be truncated below %d bytes, got %d", fullSize, repairedInfo.Size())
	}

	got2, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("second ReadAllLogEntries: %v", err)
	}
	if !reflect.DeepEqual(got2, want) {
		t.Fatalf("second read got %+v, want %+v", got2, want)
	}

	// The log must remain appendable after recovery.
	if err := AppendLogEntries(dir, []LogEntry{{Term: 3, Index: 3, Command: []byte("clean-third")}}); err != nil {
		t.Fatalf("AppendLogEntries after recovery: %v", err)
	}
	got3, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries after post-recovery append: %v", err)
	}
	wantFinal := append(append([]LogEntry{}, want...), LogEntry{Term: 3, Index: 3, Command: []byte("clean-third")})
	if !reflect.DeepEqual(got3, wantFinal) {
		t.Fatalf("got %+v, want %+v", got3, wantFinal)
	}
}

// TestReadAllLogEntries_RecoversFromTruncatedLengthPrefix simulates an even
// earlier crash: only 2 of the 4 length-prefix bytes of a new record made
// it to disk before the crash.
func TestReadAllLogEntries_RecoversFromTruncatedLengthPrefix(t *testing.T) {
	dir := t.TempDir()
	first := []LogEntry{{Term: 1, Index: 1, Command: []byte("only-entry")}}
	if err := AppendLogEntries(dir, first); err != nil {
		t.Fatalf("AppendLogEntries: %v", err)
	}
	fullInfo, err := os.Stat(logPath(dir))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	// Manually append 2 stray bytes to simulate a length prefix that got
	// cut off after only 2 of its 4 bytes were written.
	f, err := os.OpenFile(logPath(dir), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0x00, 0x01}); err != nil {
		t.Fatalf("write stray bytes: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := ReadAllLogEntries(dir)
	if err != nil {
		t.Fatalf("ReadAllLogEntries: %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("got %+v, want %+v", got, first)
	}

	repairedInfo, err := os.Stat(logPath(dir))
	if err != nil {
		t.Fatalf("stat after recovery: %v", err)
	}
	if repairedInfo.Size() != fullInfo.Size() {
		t.Fatalf("expected log truncated back to %d bytes, got %d", fullInfo.Size(), repairedInfo.Size())
	}
}

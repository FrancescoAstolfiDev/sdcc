// Package persistence implements the on-disk durability layer for a single
// Raft-lite node: the current term / vote (state.json) and the replicated
// log (log.dat). It has no networking dependencies so it can be unit tested
// in complete isolation (spec Week 2-3, build order §10.7).
package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// LogEntry is a single entry in the replicated log. The log is 1-indexed:
// index 0 means "no entries" and is never itself a real entry. An empty log
// therefore has lastLogIndex == 0 and lastLogTerm == 0.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

const (
	logFileName   = "log.dat"
	stateFileName = "state.json"
	dirPerm       = 0o755
	filePerm      = 0o644
)

func logPath(dir string) string   { return filepath.Join(dir, logFileName) }
func statePath(dir string) string { return filepath.Join(dir, stateFileName) }

type diskState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
}

// LoadState reads currentTerm/votedFor from state.json. A missing file is
// not an error: it means a brand-new node, and the zero values (term 0, no
// vote) are the correct starting state.
func LoadState(dir string) (currentTerm uint64, votedFor string, err error) {
	data, err := os.ReadFile(statePath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	var s diskState
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, "", err
	}
	return s.CurrentTerm, s.VotedFor, nil
}

// SaveState persists currentTerm/votedFor via write-to-temp-then-rename, so
// a crash mid-write never leaves a half-written state.json behind.
//
// Deliberate accepted limitation: unlike log.dat, this file is not
// synchronously fsync'd before the rename returns and the node acks the
// vote/append RPC that triggered the change. Per the project's accepted
// trade-off, the worst case is a node re-granting a vote it already granted
// after a crash right around a term change (double-voting-on-crash-restart)
// — accepted because closing it requires fsync-ing on every single vote,
// which is disproportionate to the risk for this phase.
func SaveState(dir string, currentTerm uint64, votedFor string) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}
	data, err := json.Marshal(diskState{CurrentTerm: currentTerm, VotedFor: votedFor})
	if err != nil {
		return err
	}
	path := statePath(dir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, filePerm); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// AppendLogEntries appends entries to log.dat, each length-prefixed so a
// partially-written trailing record can be detected and truncated on the
// next load instead of corrupting the whole file. It flushes and fsyncs
// before returning; callers must not ack the corresponding AppendEntries
// RPC until this returns nil.
func AppendLogEntries(dir string, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}
	f, err := os.OpenFile(logPath(dir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, e := range entries {
		record, err := encodeRecord(e)
		if err != nil {
			return err
		}
		if _, err := f.Write(record); err != nil {
			return err
		}
	}
	return f.Sync()
}

// ReadAllLogEntries reads every complete entry from log.dat, in order. If it
// encounters a trailing partial record (a length prefix or payload cut short
// by a crash mid-append), it truncates log.dat back to the end of the last
// complete record and returns the entries read so far, with no error — this
// is the recovery path for the crash-mid-append case described in §1.
func ReadAllLogEntries(dir string) ([]LogEntry, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	path := logPath(dir)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []LogEntry
	var offset int64
	for {
		var lenPrefix [4]byte
		if _, err := io.ReadFull(f, lenPrefix[:]); err != nil {
			if err == io.EOF {
				break
			}
			// Partial length prefix: truncate back to last good record.
			if err == io.ErrUnexpectedEOF {
				return entries, f.Truncate(offset)
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(lenPrefix[:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			// Partial payload (length header written, body cut short):
			// same recovery, truncate back to before this record.
			return entries, f.Truncate(offset)
		}
		var e LogEntry
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&e); err != nil {
			// Length-prefix and payload both read fully but content is
			// corrupt (e.g. torn write that happened to land on a record
			// boundary) — treat the same as a partial trailing record.
			return entries, f.Truncate(offset)
		}
		entries = append(entries, e)
		offset += int64(len(lenPrefix)) + int64(length)
	}
	return entries, nil
}

// TruncateLogFrom discards all entries with Index >= index, used by the
// leader's log-repair path (§5) to drop conflicting entries before
// re-appending the correct ones. It rewrites log.dat via
// write-to-temp-then-rename so a crash mid-rewrite can't corrupt the file.
func TruncateLogFrom(dir string, index uint64) error {
	entries, err := ReadAllLogEntries(dir)
	if err != nil {
		return err
	}
	kept := entries[:0]
	for _, e := range entries {
		if e.Index < index {
			kept = append(kept, e)
		}
	}
	return RewriteLog(dir, kept)
}

// RewriteLog atomically replaces log.dat's entire contents with entries, via
// write-to-temp-then-rename-then-fsync so a crash mid-rewrite can't corrupt
// the file. It is the shared crash-safe primitive behind TruncateLogFrom
// (drops a conflicting suffix, §5's log-repair path) and is also reused
// directly by log compaction (§5.1/§5.3, Week 5): compaction needs to drop a
// *prefix* of the log (replacing it with a single anchor placeholder entry)
// rather than a suffix, which TruncateLogFrom's index filter (`Index <
// index`) cannot express — but the underlying safe-rewrite mechanics are
// identical, so compaction builds its replacement entry slice itself and
// calls this directly instead of duplicating the temp-file/fsync/rename
// dance.
func RewriteLog(dir string, entries []LogEntry) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}
	path := logPath(dir)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	for _, e := range entries {
		record, err := encodeRecord(e)
		if err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(record); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func encodeRecord(e LogEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return nil, err
	}
	record := make([]byte, 4+buf.Len())
	binary.BigEndian.PutUint32(record[:4], uint32(buf.Len()))
	copy(record[4:], buf.Bytes())
	return record, nil
}

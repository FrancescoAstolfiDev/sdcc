// Package snapshotfile defines the on-disk/wire format for a compaction
// checkpoint (spec §5.2/§3.6): the state-machine map plus the log position
// it corresponds to. It is deliberately dependency-free (no b5-kvstore/internal/raft,
// no b5-kvstore/internal/statemachine) so both sides of the Week 5 snapshot
// pipeline can share the exact same struct definition instead of each
// re-declaring their own and risking the two silently drifting apart:
//   - internal/snapshot (Snapshot & Backup service) is the writer — it
//     builds a File after a compaction cycle and persists it via WriteFile.
//   - internal/raft (consensus node) is the reader — it decodes a File from
//     the bytes streamed back by FetchSnapshot (§9.5) to adopt as its new
//     state-machine baseline (§5.3).
package snapshotfile

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// File is the full contents of one snapshot-<lastIncludedIndex>.dat
// checkpoint (§3.6's naming convention).
type File struct {
	State             map[string]string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// Encode gob-encodes f to w.
func Encode(w io.Writer, f File) error {
	return gob.NewEncoder(w).Encode(f)
}

// Decode gob-decodes a File from r.
func Decode(r io.Reader) (File, error) {
	var f File
	if err := gob.NewDecoder(r).Decode(&f); err != nil {
		return File{}, err
	}
	return f, nil
}

// FileName returns the §3.6-conventioned file name for a snapshot whose
// last included index is lastIncludedIndex, e.g. "snapshot-150.dat".
func FileName(lastIncludedIndex uint64) string {
	return fmt.Sprintf("snapshot-%d.dat", lastIncludedIndex)
}

// WriteFile encodes f and persists it to dir/snapshot-<f.LastIncludedIndex>.dat
// via write-to-temp-then-rename, matching the crash-safety pattern already
// used by internal/raft/persistence for state.json/log.dat, and returns the
// path written.
func WriteFile(dir string, f File) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, FileName(f.LastIncludedIndex))
	var buf bytes.Buffer
	if err := Encode(&buf, f); err != nil {
		return "", err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	return path, nil
}

// ReadFile reads and decodes the snapshot file at path.
func ReadFile(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	return Decode(bytes.NewReader(data))
}

// Latest scans dir for the snapshot-*.dat file with the highest
// lastIncludedIndex and decodes it. found is false (with a zero File and
// empty path) if dir has no snapshot files yet — not an error, since that's
// the normal state for a brand-new node/service that hasn't compacted or
// backed up anything.
//
// Used both by a consensus node's own restart recovery (spec §3.6's
// startup sequence: "reads the latest snapshot-*.dat... to seed lastApplied
// and the state machine") and, as a convenience, by the Snapshot & Backup
// service to recover its own catalog after a restart — same file format,
// same "find the newest one" logic either way.
func Latest(dir string) (file File, path string, found bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return File{}, "", false, nil
		}
		return File{}, "", false, err
	}
	var bestIndex uint64
	var bestName string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var idx uint64
		if _, scanErr := fmt.Sscanf(e.Name(), "snapshot-%d.dat", &idx); scanErr != nil {
			continue
		}
		if bestName == "" || idx > bestIndex {
			bestIndex, bestName = idx, e.Name()
		}
	}
	if bestName == "" {
		return File{}, "", false, nil
	}
	path = filepath.Join(dir, bestName)
	file, err = ReadFile(path)
	if err != nil {
		return File{}, "", false, err
	}
	return file, path, true, nil
}

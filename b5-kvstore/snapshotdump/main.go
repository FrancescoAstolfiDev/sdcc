package main

import (
	"encoding/json"
	"fmt"
	"os"

	"b5-kvstore/internal/snapshotfile"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: snapshotdump <path-to-file.dat>")
		os.Exit(1)
	}

	snap, err := snapshotfile.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "read error:", err)
		os.Exit(1)
	}

	out, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "JSON error:", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

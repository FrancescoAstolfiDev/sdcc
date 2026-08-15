// Command loadgen is a small load generator for the scalability scenario
// (spec §12.1): it hits the Client Proxy's REST surface with a configurable
// mix of reads (GET) and writes (POST), records per-request latency, and
// reports p50/p95/p99 plus a raw CSV for later cross-run comparison across
// cluster sizes (3/5/7 nodes).
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// maxKeysPerWorker bounds each worker's local list of its own previously
// written keys (kept "small" per the requirement, not unbounded growth over
// a long run) — reads sample from this ring rather than the full history.
const maxKeysPerWorker = 100

// result is one measured call: what it was, when it started, how it ended,
// and how long it took. timestamp/operation/status/latencyMs map directly
// onto the CSV columns.
type result struct {
	timestamp time.Time
	operation string // "read" or "write"
	status    int    // 0 means the request never got an HTTP response (transport error)
	latencyMs float64
}

func main() {
	target := flag.String("target", "http://localhost:8080", "base URL of the Client Proxy")
	requests := flag.Int("requests", 1000, "total number of requests to issue")
	concurrency := flag.Int("concurrency", 10, "number of parallel workers")
	readPct := flag.Int("read-pct", 80, "percentage of requests that are reads (GET) vs writes (POST), 0-100")
	flag.Parse()

	if *readPct < 0 || *readPct > 100 {
		fmt.Fprintln(os.Stderr, "loadgen: -read-pct must be between 0 and 100")
		os.Exit(1)
	}
	if *requests <= 0 {
		fmt.Fprintln(os.Stderr, "loadgen: -requests must be > 0")
		os.Exit(1)
	}
	if *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "loadgen: -concurrency must be > 0")
		os.Exit(1)
	}

	// A shared client, reused across every worker: http.Client is safe for
	// concurrent use, and reusing it (with a pool sized for the requested
	// concurrency) is what lets connections actually be reused instead of
	// contending on the default transport's tiny per-host pool — with many
	// concurrent workers hitting one host, that default would itself become
	// the bottleneck and inflate every latency measurement below.
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	jobs := make(chan struct{}, *requests)
	for i := 0; i < *requests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	resultsCh := make(chan result, *requests)

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(workerID, *target, *readPct, client, jobs, resultsCh)
		}(w)
	}
	wg.Wait()
	close(resultsCh)

	all := make([]result, 0, *requests)
	for r := range resultsCh {
		all = append(all, r)
	}

	path, err := writeCSV(all)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: failed to write results CSV: %v\n", err)
		os.Exit(1)
	}

	printSummary(all)
	fmt.Printf("\nRaw results written to %s\n", path)
}

// runWorker drains jobs, issuing one HTTP call per job: a write whenever it
// has no keys of its own yet to read back (so every worker can always make
// progress regardless of -read-pct), otherwise a weighted read/write choice
// per -read-pct. Each worker keeps its own local key list — reusing a key
// this same worker actually wrote, never one owned by another worker, so a
// read never legitimately 404s and skews the read-latency stats with
// not-found noise.
func runWorker(id int, target string, readPct int, client *http.Client, jobs <-chan struct{}, out chan<- result) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(id)))
	var ownKeys []string

	for range jobs {
		if len(ownKeys) > 0 && rng.Intn(100) < readPct {
			key := ownKeys[rng.Intn(len(ownKeys))]
			out <- doGet(client, target, key)
			continue
		}

		key := fmt.Sprintf("key-%d-%d", id, time.Now().UnixNano())
		r := doPost(client, target, key)
		out <- r
		if r.status >= 200 && r.status < 300 {
			ownKeys = append(ownKeys, key)
			if len(ownKeys) > maxKeysPerWorker {
				ownKeys = ownKeys[1:] // drop the oldest, keep the list small
			}
		}
	}
}

func doGet(client *http.Client, target, key string) result {
	start := time.Now()
	resp, err := client.Get(target + "/v1/kv/" + url.PathEscape(key))
	latency := time.Since(start)
	status := drainAndStatus(resp, err)
	return result{timestamp: start, operation: "read", status: status, latencyMs: msFloat(latency)}
}

func doPost(client *http.Client, target, key string) result {
	body, _ := json.Marshal(map[string]string{"value": "loadgen-value"})
	start := time.Now()
	resp, err := client.Post(target+"/v1/kv/"+url.PathEscape(key), "application/json", bytes.NewReader(body))
	latency := time.Since(start)
	status := drainAndStatus(resp, err)
	return result{timestamp: start, operation: "write", status: status, latencyMs: msFloat(latency)}
}

// drainAndStatus fully reads and closes the response body (so the
// connection can be reused, per net/http's own requirement for that) and
// returns its status code, or 0 if the request never got a response at all
// (a transport-level error/timeout, not an HTTP error status).
func drainAndStatus(resp *http.Response, err error) int {
	if err != nil || resp == nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func msFloat(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// writeCSV persists every raw measurement to
// experiments/scalability/results-<timestamp>.csv (spec §12.1: the input
// the cross-run 3/5/7-node comparison plots are built from) and returns the
// path written.
func writeCSV(all []result) (string, error) {
	dir := filepath.Join("experiments", "scalability")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create %s: %w", dir, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("results-%s.csv", time.Now().Format("20060102-150405")))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write([]string{"timestamp", "operation", "status_code", "latency_ms"}); err != nil {
		return "", err
	}
	for _, r := range all {
		row := []string{
			r.timestamp.Format(time.RFC3339Nano),
			r.operation,
			strconv.Itoa(r.status),
			strconv.FormatFloat(r.latencyMs, 'f', 3, 64),
		}
		if err := w.Write(row); err != nil {
			return "", err
		}
	}
	w.Flush()
	return path, w.Error()
}

// printSummary prints the total request count, overall 2xx success rate,
// and p50/p95/p99 latency computed separately for reads and writes.
func printSummary(all []result) {
	var reads, writes []result
	success := 0
	for _, r := range all {
		if r.status >= 200 && r.status < 300 {
			success++
		}
		if r.operation == "read" {
			reads = append(reads, r)
		} else {
			writes = append(writes, r)
		}
	}

	fmt.Printf("Total requests: %d\n", len(all))
	if len(all) > 0 {
		fmt.Printf("Success rate (2xx): %.2f%%\n", 100*float64(success)/float64(len(all)))
	}
	fmt.Println()
	printLatencyStats("Read ", reads)
	printLatencyStats("Write", writes)
}

func printLatencyStats(label string, rs []result) {
	if len(rs) == 0 {
		fmt.Printf("%s: no requests\n", label)
		return
	}
	lat := make([]float64, len(rs))
	for i, r := range rs {
		lat[i] = r.latencyMs
	}
	sort.Float64s(lat)
	fmt.Printf("%s (n=%d): p50=%.2fms  p95=%.2fms  p99=%.2fms\n",
		label, len(rs), percentile(lat, 50), percentile(lat, 95), percentile(lat, 99))
}

// percentile returns the p-th percentile (0-100) of sorted (must already be
// sorted ascending) via nearest-rank.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

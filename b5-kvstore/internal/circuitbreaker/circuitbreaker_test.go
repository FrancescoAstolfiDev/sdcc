package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

var errBoom = errors.New("boom")

func fail() (any, error) { return nil, errBoom }
func ok() (any, error)   { return "ok", nil }

// TestTripsOnExactlyThreeConsecutiveFailures is the dedicated test required
// by md-week4 §6: confirm the breaker opens on the 3rd consecutive failure,
// not the 2nd or the 4th.
func TestTripsOnExactlyThreeConsecutiveFailures(t *testing.T) {
	r := newRegistry(nil, 2*time.Second)

	if _, err := r.Call("node-1", fail); err == nil {
		t.Fatal("expected failure 1 to propagate")
	}
	if r.State("node-1") != StateClosed {
		t.Fatalf("after 1 failure: state = %v, want Closed", r.State("node-1"))
	}

	if _, err := r.Call("node-1", fail); err == nil {
		t.Fatal("expected failure 2 to propagate")
	}
	if r.State("node-1") != StateClosed {
		t.Fatalf("after 2 failures: state = %v, want Closed (must not trip early)", r.State("node-1"))
	}

	if _, err := r.Call("node-1", fail); err == nil {
		t.Fatal("expected failure 3 to propagate")
	}
	if r.State("node-1") != StateOpen {
		t.Fatalf("after 3 consecutive failures: state = %v, want Open", r.State("node-1"))
	}
}

func TestOnTripCallbackFiresExactlyOnceOnTransitionToOpen(t *testing.T) {
	var tripped []string
	r := newRegistry(func(addr string) { tripped = append(tripped, addr) }, 2*time.Second)

	for i := 0; i < 3; i++ {
		_, _ = r.Call("node-1", fail)
	}
	if len(tripped) != 1 || tripped[0] != "node-1" {
		t.Fatalf("onTrip calls = %v, want exactly one call for node-1", tripped)
	}

	// A 4th failure while already open shouldn't even reach fn (breaker
	// short-circuits) or re-fire onTrip.
	if _, err := r.Call("node-1", fail); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected ErrOpenState while open, got %v", err)
	}
	if len(tripped) != 1 {
		t.Fatalf("onTrip should not re-fire while already open, got %v", tripped)
	}
}

// TestStaysOpenForConfiguredTimeout confirms the breaker rejects calls
// throughout the configured open Timeout and only allows a probe after it
// elapses.
func TestStaysOpenForConfiguredTimeout(t *testing.T) {
	const timeout = 80 * time.Millisecond
	r := newRegistry(nil, timeout)
	for i := 0; i < 3; i++ {
		_, _ = r.Call("node-1", fail)
	}
	if r.State("node-1") != StateOpen {
		t.Fatal("expected Open after 3 consecutive failures")
	}

	// Still open well before the timeout elapses.
	time.Sleep(timeout / 2)
	if _, err := r.Call("node-1", ok); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected ErrOpenState mid-timeout, got %v", err)
	}

	// After the timeout, the breaker must allow a probe through.
	time.Sleep(timeout)
	if r.State("node-1") != StateHalfOpen {
		t.Fatalf("state after timeout = %v, want HalfOpen", r.State("node-1"))
	}
}

// TestHalfOpenAllowsExactlyOneProbe confirms MaxRequests=1: exactly one
// request is allowed through in half-open state; a concurrent second one is
// rejected until the first probe's outcome is known.
func TestHalfOpenAllowsExactlyOneProbe(t *testing.T) {
	const timeout = 50 * time.Millisecond
	r := newRegistry(nil, timeout)
	for i := 0; i < 3; i++ {
		_, _ = r.Call("node-1", fail)
	}
	time.Sleep(timeout + 10*time.Millisecond)
	if r.State("node-1") != StateHalfOpen {
		t.Fatal("expected HalfOpen after the open timeout elapsed")
	}

	release := make(chan struct{})
	probeStarted := make(chan struct{})
	probeResult := make(chan error, 1)
	go func() {
		_, err := r.Call("node-1", func() (any, error) {
			close(probeStarted)
			<-release
			return "ok", nil
		})
		probeResult <- err
	}()
	<-probeStarted

	// A second concurrent call must be rejected: only one probe allowed.
	if _, err := r.Call("node-1", ok); !errors.Is(err, gobreaker.ErrTooManyRequests) {
		t.Fatalf("expected ErrTooManyRequests for a second half-open probe, got %v", err)
	}

	close(release)
	if err := <-probeResult; err != nil {
		t.Fatalf("probe call failed: %v", err)
	}
	if r.State("node-1") != StateClosed {
		t.Fatalf("state after successful probe = %v, want Closed", r.State("node-1"))
	}
}

func TestAllOpen(t *testing.T) {
	r := newRegistry(nil, 2*time.Second)
	if r.AllOpen([]string{"node-1", "node-2"}) {
		t.Fatal("fresh breakers must not report AllOpen")
	}
	for i := 0; i < 3; i++ {
		_, _ = r.Call("node-1", fail)
	}
	if r.AllOpen([]string{"node-1", "node-2"}) {
		t.Fatal("only one of two nodes tripped: AllOpen must be false")
	}
	for i := 0; i < 3; i++ {
		_, _ = r.Call("node-2", fail)
	}
	if !r.AllOpen([]string{"node-1", "node-2"}) {
		t.Fatal("both nodes tripped: AllOpen must be true")
	}
	if r.AllOpen(nil) {
		t.Fatal("empty address set must not report AllOpen")
	}
}

func TestPruneDropsStaleAddressesAndFreshBreakerReplacesThem(t *testing.T) {
	r := newRegistry(nil, 2*time.Second)
	for i := 0; i < 3; i++ {
		_, _ = r.Call("old-addr", fail)
	}
	if r.State("old-addr") != StateOpen {
		t.Fatal("expected old-addr breaker to be open")
	}

	r.Prune(map[string]bool{"new-addr": true})
	if r.State("old-addr") != StateClosed {
		t.Fatal("pruned address must report Closed again (fresh breaker on next use)")
	}
}

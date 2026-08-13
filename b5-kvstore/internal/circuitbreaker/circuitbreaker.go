// Package circuitbreaker wraps every outbound Client Proxy -> consensus-node
// gRPC call in a sony/gobreaker instance, one breaker per node address
// (md-week4 §5). Breakers are purely a proxy-side concern: no consensus
// node or Service Discovery instance needs to know breaker state.
package circuitbreaker

import (
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

// consecutiveFailuresToTrip, openTimeout, halfOpenMaxRequests and
// closedInterval are exactly the values from md-week4 §5's table.
const (
	consecutiveFailuresToTrip = 3
	openTimeout               = 2 * time.Second
	halfOpenMaxRequests       = 1
	closedInterval            = 30 * time.Second
)

// State re-exports gobreaker.State so callers don't need to import
// gobreaker directly just to inspect breaker state.
type State = gobreaker.State

const (
	StateClosed   = gobreaker.StateClosed
	StateHalfOpen = gobreaker.StateHalfOpen
	StateOpen     = gobreaker.StateOpen
)

// Registry manages one *gobreaker.CircuitBreaker per node address, created
// lazily on first use. A node whose address changes (per Service
// Discovery) simply gets a fresh breaker the next time that new address is
// used — see Prune, called by the proxy on every ClusterView refresh to
// drop breakers for addresses no longer live.
type Registry struct {
	onTrip  func(addr string)
	timeout time.Duration // openTimeout in production; overridable for tests only

	mu       sync.Mutex
	breakers map[string]*gobreaker.CircuitBreaker
}

// NewRegistry builds a Registry. onTrip, if non-nil, is invoked exactly
// once per breaker trip (transition into the open state) — the proxy wires
// this to trigger an out-of-schedule ClusterView refresh from Service
// Discovery (md-week4 §5), rather than waiting for the next periodic one.
func NewRegistry(onTrip func(addr string)) *Registry {
	return newRegistry(onTrip, openTimeout)
}

// newRegistry is the shared constructor; tests use a much smaller timeout
// than production's 2s so the open->half-open transition doesn't make the
// suite slow, without duplicating the trip/probe logic under test.
func newRegistry(onTrip func(addr string), timeout time.Duration) *Registry {
	return &Registry{
		onTrip:   onTrip,
		timeout:  timeout,
		breakers: make(map[string]*gobreaker.CircuitBreaker),
	}
}

func (r *Registry) breakerFor(addr string) *gobreaker.CircuitBreaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cb, ok := r.breakers[addr]; ok {
		return cb
	}
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        addr,
		MaxRequests: halfOpenMaxRequests,
		Interval:    closedInterval,
		Timeout:     r.timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= consecutiveFailuresToTrip
		},
		OnStateChange: func(name string, _, to gobreaker.State) {
			if to == gobreaker.StateOpen && r.onTrip != nil {
				r.onTrip(name)
			}
		},
	})
	r.breakers[addr] = cb
	return cb
}

// Call executes fn through the breaker for addr. If the breaker is open (or
// half-open with its single probe slot already taken), fn is not invoked at
// all and Call returns gobreaker.ErrOpenState / ErrTooManyRequests.
func (r *Registry) Call(addr string, fn func() (any, error)) (any, error) {
	return r.breakerFor(addr).Execute(fn)
}

// State reports the current state of the breaker for addr. An address with
// no breaker yet (never called) is reported closed — matching a fresh
// breaker's actual initial state.
func (r *Registry) State(addr string) State {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cb, ok := r.breakers[addr]; ok {
		return cb.State()
	}
	return gobreaker.StateClosed
}

// AllOpen reports whether every address in addrs currently has an open
// breaker — the condition md-week4 §5 maps to a 503 unavailable response.
// An empty addrs is never "all open" (there's nothing to be circuit-broken
// from); the proxy handles an empty known-node set as its own case.
func (r *Registry) AllOpen(addrs []string) bool {
	if len(addrs) == 0 {
		return false
	}
	for _, a := range addrs {
		if r.State(a) != gobreaker.StateOpen {
			return false
		}
	}
	return true
}

// Prune drops breakers for addresses not present in live. Called by the
// proxy whenever its cached ClusterView is refreshed, so that an address
// Service Discovery no longer reports (a node replaced/re-addressed) starts
// clean — a fresh, closed breaker — the next time it's (re)used, rather
// than inheriting stale trip state forever.
func (r *Registry) Prune(live map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for addr := range r.breakers {
		if !live[addr] {
			delete(r.breakers, addr)
		}
	}
}

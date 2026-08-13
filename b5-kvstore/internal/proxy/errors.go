package proxy

import "time"

// apiError is the internal representation of md-week4 §2's uniform error
// envelope ({"error": {"code": "...", "message": "..."}}), carrying enough
// information (status, retryAfter) for the HTTP layer to render it.
type apiError struct {
	status     int
	code       string
	message    string
	retryAfter time.Duration // >0 only for 503 unavailable (§2)
}

func (e *apiError) Error() string { return e.message }

func errBadRequest(msg string) *apiError {
	return &apiError{status: 400, code: "bad_request", message: msg}
}

func errNotFound(msg string) *apiError {
	return &apiError{status: 404, code: "not_found", message: msg}
}

func errUnavailable(msg string, retryAfter time.Duration) *apiError {
	return &apiError{status: 503, code: "unavailable", message: msg, retryAfter: retryAfter}
}

func errInternal(msg string) *apiError {
	return &apiError{status: 500, code: "internal_error", message: msg}
}

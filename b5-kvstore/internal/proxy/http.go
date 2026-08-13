package proxy

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// NewHTTPHandler builds the external REST surface (md-week4 §2): one path
// per key, HTTP verb encodes the operation, plus a proxy-only /healthz
// liveness endpoint (not cluster status).
func NewHTTPHandler(p *Proxy) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /v1/kv/{key}", p.handlePush)
	mux.HandleFunc("PUT /v1/kv/{key}", p.handleUpdate)
	mux.HandleFunc("GET /v1/kv/{key}", p.handleGet)
	mux.HandleFunc("DELETE /v1/kv/{key}", p.handleDelete)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// pushOrUpdateBody is {"value": "..."} — a missing "value" key is a 400
// bad_request (§2's error table), distinguished from an explicit empty
// string value via a pointer.
type pushOrUpdateBody struct {
	Value *string `json:"value"`
}

func decodeValueBody(r *http.Request) (string, *apiError) {
	var body pushOrUpdateBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return "", errBadRequest("malformed JSON body")
	}
	if body.Value == nil {
		return "", errBadRequest(`missing "value" in request body`)
	}
	return *body.Value, nil
}

func (p *Proxy) handlePush(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, aerr := decodeValueBody(r)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	reply, aerr := p.Write(r.Context(), OpPut, key, value)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	writeCommitted(w, http.StatusCreated, key, reply.GetCommitIndex())
}

func (p *Proxy) handleUpdate(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, aerr := decodeValueBody(r)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	reply, aerr := p.Write(r.Context(), OpUpdate, key, value)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	writeCommitted(w, http.StatusOK, key, reply.GetCommitIndex())
}

func (p *Proxy) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	reply, aerr := p.Write(r.Context(), OpDelete, key, "")
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	w.Header().Set("X-Commit-Index", strconv.FormatUint(reply.GetCommitIndex(), 10))
	w.WriteHeader(http.StatusNoContent)
}

func (p *Proxy) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	reply, aerr := p.Get(r.Context(), key)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": reply.GetValue()})
}

// writeCommitted renders the shared Put/Update success shape (§2): the
// commit index goes both in the JSON body (for readability) and in the
// X-Commit-Index header (so the Week 6 load-generator can read it with a
// header lookup instead of a JSON parse, per §2's note).
func writeCommitted(w http.ResponseWriter, status int, key string, commitIndex uint64) {
	w.Header().Set("X-Commit-Index", strconv.FormatUint(commitIndex, 10))
	writeJSON(w, status, map[string]any{"key": key, "commitIndex": commitIndex})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError renders md-week4 §2's uniform error envelope, including
// Retry-After for 503s (§2/§5).
func writeError(w http.ResponseWriter, aerr *apiError) {
	if aerr.retryAfter > 0 {
		secs := int(aerr.retryAfter.Seconds())
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	writeJSON(w, aerr.status, map[string]any{
		"error": map[string]string{"code": aerr.code, "message": aerr.message},
	})
}

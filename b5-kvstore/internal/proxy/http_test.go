package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"b5-kvstore/pkg/pb"
)

func newTestServer(t *testing.T, view *pb.ClusterView, factory *fakeFactory) *httptest.Server {
	t.Helper()
	p := newTestProxy(t, view, factory)
	srv := httptest.NewServer(NewHTTPHandler(p))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTP_Healthz(t *testing.T) {
	srv := newTestServer(t, &pb.ClusterView{}, newFakeFactory())
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHTTP_Push_201WithCommitIndexHeaderAndBody(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		putFn: func(context.Context, *pb.PutRequest) (*pb.WriteReply, error) {
			return &pb.WriteReply{Success: true, CommitIndex: 42}, nil
		},
	}
	srv := newTestServer(t, viewWith("leader:1"), factory)

	resp, err := http.Post(srv.URL+"/v1/kv/foo", "application/json", strings.NewReader(`{"value":"bar"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if h := resp.Header.Get("X-Commit-Index"); h != "42" {
		t.Fatalf("X-Commit-Index = %q, want 42", h)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["key"] != "foo" || body["commitIndex"].(float64) != 42 {
		t.Fatalf("body = %v", body)
	}
}

func TestHTTP_Push_MissingValue_400(t *testing.T) {
	srv := newTestServer(t, viewWith("leader:1"), newFakeFactory())
	resp, err := http.Post(srv.URL+"/v1/kv/foo", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	assertErrorEnvelope(t, resp, 400, "bad_request")
}

func TestHTTP_Push_MalformedJSON_400(t *testing.T) {
	srv := newTestServer(t, viewWith("leader:1"), newFakeFactory())
	resp, err := http.Post(srv.URL+"/v1/kv/foo", "application/json", strings.NewReader(`not json`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	assertErrorEnvelope(t, resp, 400, "bad_request")
}

func TestHTTP_Update_200(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		updateFn: func(context.Context, *pb.PutRequest) (*pb.WriteReply, error) {
			return &pb.WriteReply{Success: true, CommitIndex: 5}, nil
		},
	}
	srv := newTestServer(t, viewWith("leader:1"), factory)

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/kv/foo", strings.NewReader(`{"value":"bar"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHTTP_Get_200(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: true, Value: "bar"}, nil
		},
	}
	srv := newTestServer(t, viewWith("leader:1"), factory)

	resp, err := http.Get(srv.URL + "/v1/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["key"] != "foo" || body["value"] != "bar" {
		t.Fatalf("body = %v", body)
	}
}

func TestHTTP_Get_404(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		getFn: func(context.Context, *pb.GetRequest) (*pb.GetReply, error) {
			return &pb.GetReply{Ok: true, Found: false}, nil
		},
	}
	srv := newTestServer(t, viewWith("leader:1"), factory)

	resp, err := http.Get(srv.URL + "/v1/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	assertErrorEnvelope(t, resp, 404, "not_found")
}

func TestHTTP_Delete_204(t *testing.T) {
	factory := newFakeFactory()
	factory.clients["leader:1"] = &fakeKVClient{
		deleteFn: func(context.Context, *pb.DeleteRequest) (*pb.WriteReply, error) {
			return &pb.WriteReply{Success: true, CommitIndex: 9}, nil
		},
	}
	srv := newTestServer(t, viewWith("leader:1"), factory)

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/kv/foo", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if h := resp.Header.Get("X-Commit-Index"); h != "9" {
		t.Fatalf("X-Commit-Index = %q, want 9", h)
	}
}

func TestHTTP_Unavailable_503WithRetryAfter(t *testing.T) {
	srv := newTestServer(t, &pb.ClusterView{}, newFakeFactory()) // no known leader
	resp, err := http.Get(srv.URL + "/v1/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	assertErrorEnvelope(t, resp, 503, "unavailable")
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 503")
	}
}

func assertErrorEnvelope(t *testing.T, resp *http.Response, wantStatus int, wantCode string) {
	t.Helper()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Code != wantCode {
		t.Fatalf("error.code = %q, want %q", body.Error.Code, wantCode)
	}
	if body.Error.Message == "" {
		t.Fatal("expected a non-empty error.message")
	}
}

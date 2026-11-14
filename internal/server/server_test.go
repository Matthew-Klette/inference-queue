package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/auth"
	"github.com/Matthew-Klette/inference-queue/internal/events"
	"github.com/Matthew-Klette/inference-queue/internal/model"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
	"github.com/Matthew-Klette/inference-queue/internal/server"
	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/testutil"
	"github.com/Matthew-Klette/inference-queue/internal/worker"
)

func TestMain(m *testing.M) { testutil.Main(m) }

const adminKey = "iq_bootstrap_admin_key_for_tests"

type env struct {
	t      *testing.T
	ts     *httptest.Server
	srv    *server.Server
	q      *queue.Queue
	db     *pgxpool.Pool
	submit string // key with submit+read
	cancel context.CancelFunc
}

type opts struct {
	latency time.Duration
	noWork  bool
	srvOpts server.Options
}

func setup(t *testing.T, o opts) *env {
	t.Helper()
	pool := testutil.NewDB(t)
	keys := store.Keys{Pool: pool}
	if err := keys.EnsureBootstrap(context.Background(), store.APIKey{Name: "admin", KeyHash: auth.Hash(adminKey),
		Scopes: []string{auth.ScopeAdmin}, RateRPS: 1000, RateBurst: 1000}); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := obs.NewMetrics()
	q := &queue.Queue{Pool: pool}
	metrics.RegisterQueueDepth(func(ctx context.Context) (int, int, int, int, error) {
		d, err := q.Depth(ctx)
		return d.Ready, d.Delayed, d.Leased, d.Dead, err
	})
	s := &server.Server{Keys: keys, Batches: store.Batches{Pool: pool}, Queue: q, Metrics: metrics, Log: log,
		Ready: pool.Ping, Opts: o.srvOpts}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	if o.noWork {
		close(done)
	} else {
		w := &worker.Pool{Queue: q, Backend: model.NewFake(model.FakeConfig{Latency: o.latency}), Metrics: metrics, Log: log,
			Cfg: worker.Config{Concurrency: 4, Poll: 10 * time.Millisecond, BackoffBase: 10 * time.Millisecond}}
		go func() { defer close(done); _ = w.Run(ctx) }()
	}
	t.Cleanup(func() { cancel(); <-done })

	e := &env{t: t, ts: ts, srv: s, q: q, cancel: cancel, db: pool}
	var created struct{ Key string }
	e.do("POST", "/v1/keys", adminKey, `{"name":"client","scopes":["submit","read"]}`, nil, &created)
	e.submit = created.Key
	return e
}

func (e *env) req(method, path, key, body string, hdr map[string]string) *http.Response {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

func (e *env) do(method, path, key, body string, hdr map[string]string, out any) *http.Response {
	e.t.Helper()
	res := e.req(method, path, key, body, hdr)
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			e.t.Fatalf("decode %s: %v", data, err)
		}
	}
	res.Body = io.NopCloser(bytes.NewReader(data))
	return res
}

type batch struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Total     int    `json:"total"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Pending   int    `json:"pending"`
	Eval      *struct {
		Scored  int     `json:"scored"`
		Matched int     `json:"matched"`
		Score   float64 `json:"exact_match"`
	} `json:"eval"`
}

func (e *env) waitDone(id, key string) batch {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var b batch
		e.do("GET", "/v1/batches/"+id, key, "", nil, &b)
		if strings.HasPrefix(b.Status, "completed") {
			return b
		}
		time.Sleep(25 * time.Millisecond)
	}
	e.t.Fatal("batch did not complete")
	return batch{}
}

func TestAuthAndScopes(t *testing.T) {
	e := setup(t, opts{noWork: true})
	if res := e.do("POST", "/v1/batches", "", `{}`, nil, nil); res.StatusCode != 401 {
		t.Errorf("no key: %d", res.StatusCode)
	}
	if res := e.do("POST", "/v1/batches", "iq_wrong", `{}`, nil, nil); res.StatusCode != 401 {
		t.Errorf("bad key: %d", res.StatusCode)
	}
	if res := e.do("GET", "/v1/keys", e.submit, "", nil, nil); res.StatusCode != 403 {
		t.Errorf("submit key on admin route: %d", res.StatusCode)
	}

	var ro struct{ Key string }
	e.do("POST", "/v1/keys", adminKey, `{"name":"reader","scopes":["read"]}`, nil, &ro)
	if res := e.do("POST", "/v1/batches", ro.Key, `{"model":"m","prompts":["a"]}`, nil, nil); res.StatusCode != 403 {
		t.Errorf("read-only key submitting: %d", res.StatusCode)
	}
	if res := e.do("GET", "/v1/batches", ro.Key, "", nil, nil); res.StatusCode != 200 {
		t.Errorf("read-only key listing: %d", res.StatusCode)
	}
}

func TestKeyLifecycleNeverExposesSecretsAgain(t *testing.T) {
	e := setup(t, opts{noWork: true})
	var created struct {
		ID, Key string
		Scopes  []string
	}
	res := e.do("POST", "/v1/keys", adminKey, `{"name":"k2","scopes":["read","read","submit"]}`, nil, &created)
	if res.StatusCode != 201 || !strings.HasPrefix(created.Key, "iq_") || len(created.Scopes) != 2 {
		t.Fatalf("create: %d %+v", res.StatusCode, created)
	}
	listRes := e.req("GET", "/v1/keys", adminKey, "", nil)
	data, _ := io.ReadAll(listRes.Body)
	listRes.Body.Close()
	if bytes.Contains(data, []byte(created.Key)) || bytes.Contains(data, []byte("key_hash")) {
		t.Fatalf("listing leaks key material: %s", data)
	}
	if res := e.do("POST", "/v1/keys", adminKey, `{"name":"bad","scopes":["root"]}`, nil, nil); res.StatusCode != 400 {
		t.Errorf("unknown scope: %d", res.StatusCode)
	}
	if res := e.do("GET", "/v1/batches", created.Key, "", nil, nil); res.StatusCode != 200 {
		t.Fatalf("new key should work: %d", res.StatusCode)
	}
	if res := e.do("DELETE", "/v1/keys/"+created.ID, adminKey, "", nil, nil); res.StatusCode != 204 {
		t.Fatalf("revoke: %d", res.StatusCode)
	}
	if res := e.do("GET", "/v1/batches", created.Key, "", nil, nil); res.StatusCode != 401 {
		t.Fatalf("revoked key still works: %d", res.StatusCode)
	}
}

func TestSubmitProcessAndFetchResults(t *testing.T) {
	e := setup(t, opts{})
	var b batch
	res := e.do("POST", "/v1/batches", e.submit,
		`{"model":"fake-1","params":{"temperature":0},"prompts":["alpha","beta","gamma","delta","epsilon"]}`, nil, &b)
	if res.StatusCode != 202 || b.Total != 5 || b.Pending != 5 {
		t.Fatalf("create: %d %+v", res.StatusCode, b)
	}
	if res.Header.Get("Location") != "/v1/batches/"+b.ID || res.Header.Get("X-Request-Id") == "" {
		t.Fatalf("headers: %v", res.Header)
	}
	done := e.waitDone(b.ID, e.submit)
	if done.Status != "completed" || done.Succeeded != 5 || done.Pending != 0 {
		t.Fatalf("done = %+v", done)
	}

	rr := e.req("GET", "/v1/batches/"+b.ID+"/results?include_prompt=true", e.submit, "", nil)
	defer rr.Body.Close()
	if ct := rr.Header.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Fatalf("content type %s", ct)
	}
	var idx []int
	sc := bufio.NewScanner(rr.Body)
	for sc.Scan() {
		var r store.Result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Status != "succeeded" || r.Output == nil || !strings.HasPrefix(*r.Output, "fake-") || r.Prompt == "" {
			t.Fatalf("result = %+v", r)
		}
		idx = append(idx, r.Index)
	}
	if len(idx) != 5 || idx[0] != 0 || idx[4] != 4 {
		t.Fatalf("indices = %v", idx)
	}

	var after strings.Builder
	rr2 := e.req("GET", "/v1/batches/"+b.ID+"/results?after=2", e.submit, "", nil)
	_, _ = io.Copy(&after, rr2.Body)
	rr2.Body.Close()
	if n := strings.Count(after.String(), "\n"); n != 2 {
		t.Fatalf("?after=2 returned %d lines", n)
	}
}

func TestEvalDatasetIsScored(t *testing.T) {
	e := setup(t, opts{})
	var b batch
	e.do("POST", "/v1/batches", e.submit, `{"model":"fake-1","dataset":[
		{"ref":"q1","prompt":"echo: 4","expected":"4"},
		{"ref":"q2","prompt":"echo: Paris","expected":"paris"},
		{"ref":"q3","prompt":"echo: blue","expected":"blue"}]}`, nil, &b)
	done := e.waitDone(b.ID, e.submit)
	if done.Eval == nil || done.Eval.Scored != 3 || done.Eval.Matched != 2 {
		t.Fatalf("eval = %+v", done.Eval)
	}
}

func TestIdempotencyKey(t *testing.T) {
	e := setup(t, opts{noWork: true})
	h := map[string]string{"Idempotency-Key": "order-42"}
	var first, replay batch
	r1 := e.do("POST", "/v1/batches", e.submit, `{"model":"m","prompts":["a","b"],"params":{"x":1,"y":2}}`, h, &first)
	// Same content with different key order and whitespace is the same request.
	r2 := e.do("POST", "/v1/batches", e.submit, `{ "params":{"y":2,"x":1}, "prompts":["a","b"], "model":"m" }`, h, &replay)
	if r1.StatusCode != 202 || r2.StatusCode != 200 || replay.ID != first.ID || r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d/%d ids %s %s", r1.StatusCode, r2.StatusCode, first.ID, replay.ID)
	}
	var errBody struct{ Error struct{ Code string } }
	r3 := e.do("POST", "/v1/batches", e.submit, `{"model":"m","prompts":["a","DIFFERENT"]}`, h, &errBody)
	if r3.StatusCode != 409 || errBody.Error.Code != "idempotency_conflict" {
		t.Fatalf("conflict: %d %+v", r3.StatusCode, errBody)
	}
	var list struct{ Batches []batch }
	e.do("GET", "/v1/batches", e.submit, "", nil, &list)
	if len(list.Batches) != 1 {
		t.Fatalf("%d batches exist after replay and conflict", len(list.Batches))
	}
}

func TestValidation(t *testing.T) {
	e := setup(t, opts{noWork: true, srvOpts: server.Options{MaxItems: 3}})
	cases := map[string]string{
		"no model":         `{"prompts":["a"]}`,
		"no items":         `{"model":"m"}`,
		"both item kinds":  `{"model":"m","prompts":["a"],"dataset":[{"prompt":"b"}]}`,
		"empty prompt":     `{"model":"m","prompts":[""]}`,
		"too many":         `{"model":"m","prompts":["a","b","c","d"]}`,
		"unknown field":    `{"model":"m","prompts":["a"],"bogus":1}`,
		"params not obj":   `{"model":"m","prompts":["a"],"params":[1]}`,
		"bad webhook":      `{"model":"m","prompts":["a"],"webhook_url":"ftp://x"}`,
		"bad max attempts": `{"model":"m","prompts":["a"],"max_attempts":99}`,
		"not json":         `nope`,
		"trailing data":    `{"model":"m","prompts":["a"]} {}`,
	}
	for name, body := range cases {
		if res := e.do("POST", "/v1/batches", e.submit, body, nil, nil); res.StatusCode != 400 {
			t.Errorf("%s: status %d", name, res.StatusCode)
		}
	}
}

func TestBatchesAreVisibleOnlyToTheirOwnerAndAdmins(t *testing.T) {
	e := setup(t, opts{noWork: true})
	var mine batch
	e.do("POST", "/v1/batches", e.submit, `{"model":"m","prompts":["a"]}`, nil, &mine)
	var other struct{ Key string }
	e.do("POST", "/v1/keys", adminKey, `{"name":"other"}`, nil, &other)

	for _, p := range []string{"", "/events", "/results"} {
		if res := e.do("GET", "/v1/batches/"+mine.ID+p, other.Key, "", nil, nil); res.StatusCode != 404 {
			t.Errorf("other key GET %q: %d", p, res.StatusCode)
		}
	}
	var list struct{ Batches []batch }
	e.do("GET", "/v1/batches", other.Key, "", nil, &list)
	if len(list.Batches) != 0 {
		t.Fatal("list leaks other keys' batches")
	}
	if res := e.do("GET", "/v1/batches/"+mine.ID, adminKey, "", nil, nil); res.StatusCode != 200 {
		t.Errorf("admin: %d", res.StatusCode)
	}
	if res := e.do("GET", "/v1/batches/not-a-uuid", e.submit, "", nil, nil); res.StatusCode != 404 {
		t.Errorf("bad id: %d", res.StatusCode)
	}
}

func TestRateLimitPerKey(t *testing.T) {
	e := setup(t, opts{noWork: true})
	var lim struct{ Key string }
	e.do("POST", "/v1/keys", adminKey, `{"name":"slow","rate_rps":0.5,"rate_burst":2}`, nil, &lim)
	codes := []int{}
	var retry string
	for i := 0; i < 4; i++ {
		res := e.do("GET", "/v1/batches", lim.Key, "", nil, nil)
		codes = append(codes, res.StatusCode)
		if res.StatusCode == 429 {
			retry = res.Header.Get("Retry-After")
		}
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 || retry == "" {
		t.Fatalf("codes=%v retry-after=%q", codes, retry)
	}
	if res := e.do("GET", "/v1/batches", e.submit, "", nil, nil); res.StatusCode != 200 {
		t.Fatalf("another key must not be affected: %d", res.StatusCode)
	}
}

func TestSSEStreamsProgressUntilDone(t *testing.T) {
	e := setup(t, opts{latency: 30 * time.Millisecond, srvOpts: server.Options{SSEInterval: 20 * time.Millisecond}})
	prompts := make([]string, 12)
	for i := range prompts {
		prompts[i] = "p" + string(rune('a'+i))
	}
	body, _ := json.Marshal(map[string]any{"model": "m", "prompts": prompts})
	var b batch
	e.do("POST", "/v1/batches", e.submit, string(body), nil, &b)

	res := e.req("GET", "/v1/batches/"+b.ID+"/events", e.submit, "", nil)
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %s", ct)
	}
	var events []string
	var last batch
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "event: "); ok {
			events = append(events, v)
		}
		if v, ok := strings.CutPrefix(line, "data: "); ok {
			_ = json.Unmarshal([]byte(v), &last)
		}
	}
	if len(events) < 2 || events[0] != "progress" || events[len(events)-1] != "done" {
		t.Fatalf("events = %v", events)
	}
	if last.Succeeded != 12 || !strings.HasPrefix(last.Status, "completed") {
		t.Fatalf("final event = %+v", last)
	}
}

func TestSSEEndsOnServerShutdown(t *testing.T) {
	shutdown, stop := context.WithCancel(context.Background())
	e := setup(t, opts{noWork: true, srvOpts: server.Options{SSEInterval: 20 * time.Millisecond, ShutdownContext: shutdown}})
	var b batch
	e.do("POST", "/v1/batches", e.submit, `{"model":"m","prompts":["a"]}`, nil, &b)
	res := e.req("GET", "/v1/batches/"+b.ID+"/events", e.submit, "", nil)
	defer res.Body.Close()
	time.AfterFunc(100*time.Millisecond, stop)
	data, _ := io.ReadAll(res.Body) // returns only if the server closes the stream
	if !bytes.Contains(data, []byte("event: shutdown")) {
		t.Fatalf("stream = %q", data)
	}
}

func TestReplayDeadLetters(t *testing.T) {
	e := setup(t, opts{})
	var b batch
	e.do("POST", "/v1/batches", e.submit, `{"model":"m","prompts":["ok","bad [[permanent]]"],"max_attempts":1}`, nil, &b)
	done := e.waitDone(b.ID, e.submit)
	if done.Status != "completed_with_errors" || done.Failed != 1 {
		t.Fatalf("done = %+v", done)
	}
	if res := e.do("POST", "/v1/batches/"+b.ID+"/replay", e.submit, "", nil, nil); res.StatusCode != 403 {
		t.Fatalf("replay needs admin: %d", res.StatusCode)
	}
	var out struct{ Replayed int }
	e.do("POST", "/v1/batches/"+b.ID+"/replay", adminKey, "", nil, &out)
	if out.Replayed != 1 {
		t.Fatalf("replayed %d", out.Replayed)
	}
	// The poisoned prompt fails again, which proves it was actually re-queued and re-run.
	deadline := time.Now().Add(10 * time.Second)
	var again batch
	for time.Now().Before(deadline) {
		e.do("GET", "/v1/batches/"+b.ID, e.submit, "", nil, &again)
		if strings.HasPrefix(again.Status, "completed") {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if again.Failed != 1 || again.Succeeded != 1 {
		t.Fatalf("after replay: %+v", again)
	}
}

func TestOperationalEndpoints(t *testing.T) {
	e := setup(t, opts{})
	var b batch
	e.do("POST", "/v1/batches", e.submit, `{"model":"m","prompts":["a","b"]}`, nil, &b)
	e.waitDone(b.ID, e.submit)

	if res := e.do("GET", "/healthz", "", "", nil, nil); res.StatusCode != 200 {
		t.Errorf("healthz %d", res.StatusCode)
	}
	if res := e.do("GET", "/readyz", "", "", nil, nil); res.StatusCode != 200 {
		t.Errorf("readyz %d", res.StatusCode)
	}
	res := e.req("GET", "/metrics", "", "", nil)
	data, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, want := range []string{"iq_queue_items{state=\"ready\"}", "iq_model_duration_seconds_bucket", "iq_lease_duration_seconds_bucket",
		"iq_http_requests_total{route=\"POST /v1/batches\",status=\"202\"} 1", "iq_items_processed_total{outcome=\"succeeded\"} 2"} {
		if !bytes.Contains(data, []byte(want)) {
			t.Errorf("metrics missing %q", want)
		}
	}
	res = e.req("GET", "/healthz", "", "", map[string]string{"X-Request-Id": "trace-me"})
	res.Body.Close()
	if res.Header.Get("X-Request-Id") != "trace-me" {
		t.Errorf("request id not propagated: %q", res.Header.Get("X-Request-Id"))
	}
}

func TestReadyzFailsWhenDependencyDown(t *testing.T) {
	e := setup(t, opts{noWork: true})
	e.srv.Ready = func(context.Context) error { return context.DeadlineExceeded }
	ts := httptest.NewServer(e.srv.Handler())
	defer ts.Close()
	res, _ := http.Get(ts.URL + "/readyz")
	res.Body.Close()
	if res.StatusCode != 503 {
		t.Fatalf("readyz = %d", res.StatusCode)
	}
}

func TestWebhookDeliveredEndToEnd(t *testing.T) {
	var got atomic.Pointer[map[string]any]
	var sigOK atomic.Bool
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sigOK.Store(events.Verify("whsec", r.Header.Get("X-IQ-Signature"), body, time.Now(), time.Minute) == nil)
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		got.Store(&m)
	}))
	defer hook.Close()

	e := setup(t, opts{})
	var b batch
	body := `{"model":"m","prompts":["a","b"],"webhook_url":"` + hook.URL + `"}`
	e.do("POST", "/v1/batches", e.submit, body, nil, &b)
	e.waitDone(b.ID, e.submit)

	p := &events.Publisher{Pool: e.db, Secret: "whsec", Interval: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for got.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	m := got.Load()
	if m == nil {
		t.Fatal("webhook never received")
	}
	if (*m)["batch_id"] != b.ID || (*m)["status"] != "completed" || (*m)["succeeded"] != float64(2) || !sigOK.Load() {
		t.Fatalf("payload = %v signature ok = %v", *m, sigOK.Load())
	}
}

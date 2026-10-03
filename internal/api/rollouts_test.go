package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Bob-xisuke/orb-rollout-probe/internal/store"
)

func validRolloutBody() string {
	return `{
		"namespace":"payments",
		"workload":"gateway",
		"targetVersion":"v2.3.0",
		"batchSize":4,
		"maxUnavailableRatio":0.25,
		"completedBatches":2,
		"probe":{"type":"httpGet","path":"/ready","port":8080,"timeoutSeconds":3,"successThreshold":1,"failureThreshold":3},
		"probeResults":[
			{"batch":0,"ready":true,"failedPods":0},
			{"batch":1,"ready":false,"failedPods":2,"reason":"timeout"}
		],
		"rollback":false
	}`
}

func newTestRouter(t *testing.T) (*store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, NewRouter(st)
}

func postRollout(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/rollouts", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recorder, request)
	return recorder
}

func errorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error body %q: %v", recorder.Body.String(), err)
	}
	return envelope.Error.Code
}

func TestPostRolloutCreatesAndReturnsFullRecord(t *testing.T) {
	_, handler := newTestRouter(t)
	recorder := postRollout(t, handler, validRolloutBody())

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	var record map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := record["id"].(string); !ok {
		t.Fatalf("response must include stable id: %v", record)
	}
	for _, field := range []string{"namespace", "workload", "targetVersion", "batchSize", "maxUnavailableRatio", "completedBatches", "probe", "probeResults", "rollback"} {
		if _, ok := record[field]; !ok {
			t.Fatalf("response missing field %q: %v", field, record)
		}
	}
	probe := record["probe"].(map[string]any)
	for _, field := range []string{"type", "path", "port", "timeoutSeconds", "successThreshold", "failureThreshold"} {
		if _, ok := probe[field]; !ok {
			t.Fatalf("probe missing field %q: %v", field, probe)
		}
	}
	if len(record["probeResults"].([]any)) != 2 {
		t.Fatalf("probeResults = %v", record["probeResults"])
	}
}

func TestPostRolloutIdenticalRepeatReturnsSameRecordWith200(t *testing.T) {
	_, handler := newTestRouter(t)
	first := postRollout(t, handler, validRolloutBody())
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d", first.Code)
	}

	// Different whitespace and key order must still count as identical content.
	reordered := `{"rollback":false,"probeResults":[{"failedPods":0,"ready":true,"batch":0},{"reason":"timeout","failedPods":2,"ready":false,"batch":1}],` +
		`"probe":{"failureThreshold":3,"successThreshold":1,"timeoutSeconds":3,"port":8080,"path":"/ready","type":"httpGet"},` +
		`"completedBatches":2,"maxUnavailableRatio":0.25,"batchSize":4,"targetVersion":"v2.3.0","workload":"gateway","namespace":"payments"}`
	repeat := postRollout(t, handler, reordered)
	if repeat.Code != http.StatusOK {
		t.Fatalf("repeat status = %d, want 200, body = %s", repeat.Code, repeat.Body.String())
	}
	if first.Body.String() != repeat.Body.String() {
		t.Fatalf("repeat body = %s, want original %s", repeat.Body.String(), first.Body.String())
	}
}

func TestPostRolloutDifferentContentReturns409(t *testing.T) {
	_, handler := newTestRouter(t)
	postRollout(t, handler, validRolloutBody())

	conflict := strings.Replace(validRolloutBody(), `"batchSize":4`, `"batchSize":8`, 1)
	recorder := postRollout(t, handler, conflict)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
	if code := errorCode(t, recorder); code != "RolloutConflictError" {
		t.Fatalf("code = %q, want RolloutConflictError", code)
	}
}

func TestPostRolloutConcurrentIdenticalBodiesCreateOneRow(t *testing.T) {
	st, handler := newTestRouter(t)
	const goroutines = 20
	var wg sync.WaitGroup
	statuses := make(chan int, goroutines)
	ids := make(chan string, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			recorder := postRollout(t, handler, validRolloutBody())
			statuses <- recorder.Code
			var record struct {
				ID string `json:"id"`
			}
			json.Unmarshal(recorder.Body.Bytes(), &record)
			ids <- record.ID
		}()
	}
	wg.Wait()
	close(statuses)
	close(ids)

	created, ok200 := 0, 0
	firstID := ""
	for status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
			ok200++
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	if created != 1 || ok200 != goroutines-1 {
		t.Fatalf("created = %d, idempotent = %d", created, ok200)
	}
	for id := range ids {
		if id == "" {
			t.Fatal("every response must carry the same id")
		}
		if firstID == "" {
			firstID = id
		} else if id != firstID {
			t.Fatalf("id = %q, want %q", id, firstID)
		}
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/rollouts?namespace=payments&workload=gateway&targetVersion=v2.3.0", nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), firstID) {
		t.Fatalf("get body = %s, want id %s", recorder.Body.String(), firstID)
	}
	_ = st
}

func TestGetRolloutHitsAndMisses(t *testing.T) {
	_, handler := newTestRouter(t)
	postRollout(t, handler, validRolloutBody())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/rollouts?namespace=payments&workload=gateway&targetVersion=v2.3.0", nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("hit status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"id":"`) {
		t.Fatalf("hit body must be full record: %s", recorder.Body.String())
	}

	miss := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/v1/rollouts?namespace=payments&workload=gateway&targetVersion=v9.9.9", nil)
	handler.ServeHTTP(miss, request)
	if miss.Code != http.StatusNotFound {
		t.Fatalf("miss status = %d, want 404", miss.Code)
	}
	if code := errorCode(t, miss); code != "RolloutNotFoundError" {
		t.Fatalf("code = %q", code)
	}
}

func TestGetRolloutRequiresAllQueryParameters(t *testing.T) {
	_, handler := newTestRouter(t)
	queries := []string{
		"/v1/rollouts",
		"/v1/rollouts?namespace=payments",
		"/v1/rollouts?workload=gateway&targetVersion=v2.3.0",
		"/v1/rollouts?namespace=&workload=gateway&targetVersion=v2.3.0",
		"/v1/rollouts?namespace=payments&workload=&targetVersion=v2.3.0",
		"/v1/rollouts?namespace=payments&workload=gateway&targetVersion=",
	}
	for _, query := range queries {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, query, nil)
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want 400", query, recorder.Code)
		}
		if code := errorCode(t, recorder); code != "InvalidRolloutInputError" {
			t.Fatalf("GET %s code = %q", query, code)
		}
	}
}

func TestPostRolloutValidatesInput(t *testing.T) {
	_, handler := newTestRouter(t)

	cases := map[string]string{
		"malformed json":        `{"namespace":`,
		"multiple json values":  validRolloutBody() + `{"namespace":"x"}`,
		"missing namespace":     strings.Replace(validRolloutBody(), `"namespace":"payments",`, "", 1),
		"blank namespace":       strings.Replace(validRolloutBody(), `"namespace":"payments"`, `"namespace":"   "`, 1),
		"missing workload":      strings.Replace(validRolloutBody(), `"workload":"gateway",`, "", 1),
		"missing targetVersion": strings.Replace(validRolloutBody(), `"targetVersion":"v2.3.0",`, "", 1),
		"zero batchSize":        strings.Replace(validRolloutBody(), `"batchSize":4`, `"batchSize":0`, 1),
		"negative batchSize":    strings.Replace(validRolloutBody(), `"batchSize":4`, `"batchSize":-1`, 1),
		"missing batchSize":     strings.Replace(validRolloutBody(), `"batchSize":4,`, "", 1),
		"ratio above one":       strings.Replace(validRolloutBody(), `"maxUnavailableRatio":0.25`, `"maxUnavailableRatio":1.1`, 1),
		"ratio below zero":      strings.Replace(validRolloutBody(), `"maxUnavailableRatio":0.25`, `"maxUnavailableRatio":-0.1`, 1),
		"missing ratio":         strings.Replace(validRolloutBody(), `"maxUnavailableRatio":0.25,`, "", 1),
		"completed too large":   strings.Replace(validRolloutBody(), `"completedBatches":2`, `"completedBatches":5`, 1),
		"completed negative":    strings.Replace(validRolloutBody(), `"completedBatches":2`, `"completedBatches":-1`, 1),
		"missing rollback":      strings.Replace(validRolloutBody(), `"rollback":false`, "", 1),
		"missing probe":         strings.Replace(validRolloutBody(), `"probe":{"type":"httpGet","path":"/ready","port":8080,"timeoutSeconds":3,"successThreshold":1,"failureThreshold":3},`, "", 1),
		"null probe":            strings.Replace(validRolloutBody(), `"probe":{"type":"httpGet","path":"/ready","port":8080,"timeoutSeconds":3,"successThreshold":1,"failureThreshold":3}`, `"probe":null`, 1),
		"probe missing type":    strings.Replace(validRolloutBody(), `"type":"httpGet",`, "", 1),
		"probe blank path":      strings.Replace(validRolloutBody(), `"path":"/ready"`, `"path":"  "`, 1),
		"probe missing port":    strings.Replace(validRolloutBody(), `"port":8080,`, "", 1),
		"probe port zero":       strings.Replace(validRolloutBody(), `"port":8080`, `"port":0`, 1),
		"probe port too large":  strings.Replace(validRolloutBody(), `"port":8080`, `"port":70000`, 1),
		"probe missing timeout": strings.Replace(validRolloutBody(), `"timeoutSeconds":3,`, "", 1),
		"probe zero timeout":    strings.Replace(validRolloutBody(), `"timeoutSeconds":3`, `"timeoutSeconds":0`, 1),
		"probe missing success": strings.Replace(validRolloutBody(), `"successThreshold":1,`, "", 1),
		"probe zero success":    strings.Replace(validRolloutBody(), `"successThreshold":1`, `"successThreshold":0`, 1),
		"probe missing failure": strings.Replace(validRolloutBody(), `,"failureThreshold":3`, "", 1),
		"probe zero failure":    strings.Replace(validRolloutBody(), `"failureThreshold":3`, `"failureThreshold":0`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			recorder := postRollout(t, handler, body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", recorder.Code, recorder.Body.String())
			}
			if code := errorCode(t, recorder); code != "InvalidRolloutInputError" {
				t.Fatalf("code = %q, want InvalidRolloutInputError", code)
			}
			lower := strings.ToLower(recorder.Body.String())
			for _, leaked := range []string{"sql", "goroutine", ".go:", "internal/store", "internal/api", "/"} {
				if strings.Contains(lower, leaked) {
					t.Fatalf("error body may leak internals (%q): %s", leaked, recorder.Body.String())
				}
			}
		})
	}
}

func TestPostRolloutBoundaryValuesAreAccepted(t *testing.T) {
	_, handler := newTestRouter(t)

	boundary := strings.Replace(validRolloutBody(), `"namespace":"payments"`, `"namespace":"edges"`, 1)
	boundary = strings.Replace(boundary, `"maxUnavailableRatio":0.25`, `"maxUnavailableRatio":0`, 1)
	boundary = strings.Replace(boundary, `"completedBatches":2`, `"completedBatches":0`, 1)
	boundary = strings.Replace(boundary, `"targetVersion":"v2.3.0"`, `"targetVersion":"zero"`, 1)
	if recorder := postRollout(t, handler, boundary); recorder.Code != http.StatusCreated {
		t.Fatalf("lower boundary status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	upper := strings.Replace(validRolloutBody(), `"namespace":"payments"`, `"namespace":"edges"`, 1)
	upper = strings.Replace(upper, `"maxUnavailableRatio":0.25`, `"maxUnavailableRatio":1`, 1)
	upper = strings.Replace(upper, `"completedBatches":2`, `"completedBatches":4`, 1)
	upper = strings.Replace(upper, `"targetVersion":"v2.3.0"`, `"targetVersion":"full"`, 1)
	if recorder := postRollout(t, handler, upper); recorder.Code != http.StatusCreated {
		t.Fatalf("upper boundary status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestErrorBodiesKeepPublishedShape(t *testing.T) {
	_, handler := newTestRouter(t)
	recorder := postRollout(t, handler, `{"namespace":"x"}`)
	var envelope map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj, ok := envelope["error"].(map[string]any)
	if !ok {
		t.Fatalf("error envelope missing: %v", envelope)
	}
	if errObj["code"] != "InvalidRolloutInputError" {
		t.Fatalf("code = %v", errObj["code"])
	}
	if message, ok := errObj["message"].(string); !ok || message == "" {
		t.Fatalf("message must be non-empty string: %v", errObj)
	}
}

func TestBaselineSurfaceIsUnchanged(t *testing.T) {
	_, handler := newTestRouter(t)

	healthz := httptest.NewRecorder()
	handler.ServeHTTP(healthz, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if healthz.Code != http.StatusOK || healthz.Body.String() != `{"database":"ok","status":"ok"}` {
		t.Fatalf("healthz changed: %d %s", healthz.Code, healthz.Body.String())
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/v1/other", nil))
	if missing.Code != http.StatusNotFound || errorCode(t, missing) != "route_not_found" {
		t.Fatalf("unknown route changed: %d %s", missing.Code, missing.Body.String())
	}

	// rollouts collection is not addressable by id; it must not shadow NoRoute.
	single := httptest.NewRecorder()
	handler.ServeHTTP(single, httptest.NewRequest(http.MethodGet, "/v1/rollouts/abc", nil))
	if single.Code != http.StatusNotFound {
		t.Fatalf("sub-path status = %d, want 404", single.Code)
	}
}

func TestPostRolloutRejectsUnknownMethodsWithExistingShape(t *testing.T) {
	_, handler := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/v1/rollouts?namespace=payments&workload=gateway&targetVersion=v2.3.0", bytes.NewReader(nil))
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (records cannot be deleted)", recorder.Code)
	}
}

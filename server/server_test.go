package loopback

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

const testToken = "0123456789abcdef-test-token"

func newTestServer(t *testing.T) (*Server, http.Handler) {
	t.Helper()
	config := Config{
		DataFile: filepath.Join(t.TempDir(), "state.json"),
		APIToken: testToken,
		Channels: []Channel{{
			ID: "jack-discord", QualifiedName: "jack:discord",
			DisplayName: "Jack", EchoPrefix: "Echo",
		}},
	}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return server, server.Handler()
}

func request(t *testing.T, handler http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestSendIsDurableAndIdempotent(t *testing.T) {
	server, handler := newTestServer(t)
	body := []byte(`{"v":1,"kind":"command","type":"message.send","command_id":"cmd-1","body":{"client_message_id":"client-1","channel_id":"jack-discord","text":"hello"}}`)

	first := request(t, handler, http.MethodPost, "/v1/messages", body)
	if first.Code != http.StatusOK {
		t.Fatalf("first send status = %d: %s", first.Code, first.Body.String())
	}
	second := request(t, handler, http.MethodPost, "/v1/messages", body)
	if second.Code != http.StatusOK {
		t.Fatalf("duplicate send status = %d: %s", second.Code, second.Body.String())
	}
	var ack map[string]interface{}
	if err := json.Unmarshal(second.Body.Bytes(), &ack); err != nil {
		t.Fatal(err)
	}
	if duplicate, _ := ack["duplicate"].(bool); !duplicate {
		t.Fatal("second response was not marked duplicate")
	}
	if len(server.state.Messages) != 2 || len(server.state.Events) != 2 {
		t.Fatalf("got %d messages and %d events, want 2 each", len(server.state.Messages), len(server.state.Events))
	}

	reloaded, err := New(server.config)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.state.Messages) != 2 {
		t.Fatalf("reloaded %d messages, want 2", len(reloaded.state.Messages))
	}
}

func TestBootstrapAndCursorSync(t *testing.T) {
	_, handler := newTestServer(t)
	body := []byte(`{"v":1,"kind":"command","type":"message.send","command_id":"cmd-1","body":{"client_message_id":"client-1","channel_id":"jack-discord","text":"hello"}}`)
	request(t, handler, http.MethodPost, "/v1/messages", body)

	bootstrap := request(t, handler, http.MethodGet, "/v1/bootstrap", nil)
	if bootstrap.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d", bootstrap.Code)
	}
	if !bytes.Contains(bootstrap.Body.Bytes(), []byte(`"cursor":"2"`)) {
		t.Fatalf("unexpected bootstrap: %s", bootstrap.Body.String())
	}

	sync := request(t, handler, http.MethodGet, "/v1/sync?after=1", nil)
	if sync.Code != http.StatusOK {
		t.Fatalf("sync status = %d", sync.Code)
	}
	var response struct {
		Events []Event `json:"events"`
	}
	if err := json.Unmarshal(sync.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Events) != 1 || response.Events[0].Cursor != "2" {
		t.Fatalf("events = %#v, want only cursor 2", response.Events)
	}
}

func TestEmptyBootstrapUsesArrays(t *testing.T) {
	_, handler := newTestServer(t)
	response := request(t, handler, http.MethodGet, "/v1/bootstrap", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d", response.Code)
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"messages":[]`)) {
		t.Fatalf("empty messages were not an array: %s", response.Body.String())
	}
}

func TestAuthenticationAndValidation(t *testing.T) {
	_, handler := newTestServer(t)
	r := httptest.NewRequest(http.MethodGet, "/v1/bootstrap", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", w.Code)
	}

	bad := []byte(`{"v":1,"kind":"command","type":"message.send","command_id":"cmd-1","body":{"client_message_id":"client-1","channel_id":"missing","text":"hello"}}`)
	w = request(t, handler, http.MethodPost, "/v1/messages", bad)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown channel status = %d, want 404", w.Code)
	}
}

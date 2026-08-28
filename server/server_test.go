package flipmessenger

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
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

func TestLongPollReturnsWhenMessageIsCommitted(t *testing.T) {
	_, handler := newTestServer(t)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		result <- request(t, handler, http.MethodGet, "/v1/sync?after=0&wait=2s", nil)
	}()

	body := []byte(`{"v":1,"kind":"command","type":"message.send","command_id":"cmd-long-poll","body":{"client_message_id":"client-long-poll","channel_id":"jack-discord","text":"wake up"}}`)
	request(t, handler, http.MethodPost, "/v1/messages", body)

	select {
	case response := <-result:
		if response.Code != http.StatusOK {
			t.Fatalf("long poll status = %d: %s", response.Code, response.Body.String())
		}
		var sync syncResponse
		if err := json.Unmarshal(response.Body.Bytes(), &sync); err != nil {
			t.Fatal(err)
		}
		if len(sync.Events) != 2 || sync.HighWatermark != "2" {
			t.Fatalf("long poll response = %#v, want two events through cursor 2", sync)
		}
	case <-time.After(time.Second):
		t.Fatal("long poll did not return after a message commit")
	}
}

func TestLongPollTimesOutAndValidatesWait(t *testing.T) {
	_, handler := newTestServer(t)
	started := time.Now()
	response := request(t, handler, http.MethodGet, "/v1/sync?after=0&wait=20ms", nil)
	if response.Code != http.StatusOK || time.Since(started) < 10*time.Millisecond {
		t.Fatalf("long poll status=%d duration=%s body=%s", response.Code, time.Since(started), response.Body.String())
	}
	response = request(t, handler, http.MethodGet, "/v1/sync?after=0&wait=26s", nil)
	if response.Code != http.StatusBadRequest || !bytes.Contains(response.Body.Bytes(), []byte(`"invalid_wait"`)) {
		t.Fatalf("invalid wait response status=%d body=%s", response.Code, response.Body.String())
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

func TestGoogleConversationAndIncomingMessage(t *testing.T) {
	server, handler := newTestServer(t)
	conversation := &gmproto.Conversation{
		ConversationID:    "conversation-1",
		Name:              "RCS Group",
		Type:              gmproto.ConversationType_RCS,
		DefaultOutgoingID: "self-participant",
	}
	if err := server.upsertGoogleConversation(conversation); err != nil {
		t.Fatal(err)
	}
	remote := &gmproto.Message{
		MessageID:      "remote-1",
		ConversationID: "conversation-1",
		Timestamp:      1_700_000_000_000_000,
		MessageStatus:  &gmproto.MessageStatus{Status: gmproto.MessageStatusType_INCOMING_COMPLETE},
		MessageInfo: []*gmproto.MessageInfo{{
			Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "hello from RCS"}},
		}},
	}
	if err := server.importGoogleMessage(remote); err != nil {
		t.Fatal(err)
	}
	if err := server.importGoogleMessage(remote); err != nil {
		t.Fatal(err)
	}

	channelID := googleChannelID("conversation-1")
	if len(server.state.Messages) != 1 {
		t.Fatalf("messages = %d, want one deduplicated message", len(server.state.Messages))
	}
	if message := server.state.Messages[0]; message.ChannelID != channelID || message.Author != "channel" || message.Text != "hello from RCS" {
		t.Fatalf("unexpected imported message: %#v", message)
	}
	sync := request(t, handler, http.MethodGet, "/v1/sync", nil)
	if !bytes.Contains(sync.Body.Bytes(), []byte(`"display_name":"RCS Group (RCS)"`)) {
		t.Fatalf("sync did not contain dynamic channel: %s", sync.Body.String())
	}
}

func TestGoogleSendIsDurablyQueuedAndRemoteEchoDeduplicates(t *testing.T) {
	server, handler := newTestServer(t)
	conversation := &gmproto.Conversation{ConversationID: "conversation-2", Name: "Jack"}
	if err := server.upsertGoogleConversation(conversation); err != nil {
		t.Fatal(err)
	}
	// A minimal manager is enough for this unit test: WakeOutbox only enqueues work.
	server.google = &GoogleMessages{}
	channelID := googleChannelID("conversation-2")
	body := []byte(`{"v":1,"kind":"command","type":"message.send","command_id":"cmd-google","body":{"client_message_id":"client-google","channel_id":"` + channelID + `","text":"hello"}}`)

	response := request(t, handler, http.MethodPost, "/v1/messages", body)
	if response.Code != http.StatusOK {
		t.Fatalf("send status = %d: %s", response.Code, response.Body.String())
	}
	if len(server.state.Outbox) != 1 || len(server.state.Messages) != 1 {
		t.Fatalf("outbox=%d messages=%d, want one each", len(server.state.Outbox), len(server.state.Messages))
	}
	remote := &gmproto.Message{
		MessageID:      "remote-outgoing",
		ConversationID: "conversation-2",
		TmpID:          "client-google",
		MessageStatus:  &gmproto.MessageStatus{Status: gmproto.MessageStatusType_OUTGOING_COMPLETE},
		MessageInfo: []*gmproto.MessageInfo{{
			Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "hello"}},
		}},
	}
	if err := server.importGoogleMessage(remote); err != nil {
		t.Fatal(err)
	}
	if len(server.state.Messages) != 1 {
		t.Fatalf("remote echo created a duplicate; messages=%d", len(server.state.Messages))
	}
	if len(server.state.Outbox) != 0 {
		t.Fatalf("remote echo did not complete outbox: %#v", server.state.Outbox)
	}
	if server.state.GoogleMessages["remote-outgoing"] != server.state.Messages[0].ID {
		t.Fatal("remote message ID was not linked to the local message")
	}
}

func TestGoogleWhitelistFiltersStoredAndNewData(t *testing.T) {
	server, handler := newTestServer(t)
	allowed := googleTestConversation("allowed-conversation", "Allowed", "+1 (555) 123-4567")
	blocked := googleTestConversation("blocked-conversation", "Blocked", "+1 555 987 6543")
	for _, conversation := range []*gmproto.Conversation{allowed, blocked} {
		if err := server.upsertGoogleConversation(conversation); err != nil {
			t.Fatal(err)
		}
		if err := server.importGoogleMessage(googleTestMessage(conversation.GetConversationID())); err != nil {
			t.Fatal(err)
		}
	}
	if len(server.state.Messages) != 2 {
		t.Fatalf("messages before filtering = %d, want 2", len(server.state.Messages))
	}

	server.google = &GoogleMessages{config: GoogleMessagesConfig{Whitelist: []string{"+15551234567"}}}
	if err := server.applyGoogleWhitelist(); err != nil {
		t.Fatal(err)
	}

	var bootstrap struct {
		Channels []struct {
			ID string `json:"channel_id"`
		} `json:"channels"`
		Messages []Message `json:"messages"`
	}
	response := request(t, handler, http.MethodGet, "/v1/bootstrap", nil)
	if err := json.Unmarshal(response.Body.Bytes(), &bootstrap); err != nil {
		t.Fatal(err)
	}
	if len(bootstrap.Channels) != 2 || bootstrap.Channels[1].ID != googleChannelID("allowed-conversation") {
		t.Fatalf("filtered channels = %#v", bootstrap.Channels)
	}
	if len(bootstrap.Messages) != 1 || bootstrap.Messages[0].ChannelID != googleChannelID("allowed-conversation") {
		t.Fatalf("filtered messages = %#v", bootstrap.Messages)
	}

	var syncResponse struct {
		Events []Event `json:"events"`
	}
	response = request(t, handler, http.MethodGet, "/v1/sync?after=0", nil)
	if err := json.Unmarshal(response.Body.Bytes(), &syncResponse); err != nil {
		t.Fatal(err)
	}
	if len(syncResponse.Events) != 1 || syncResponse.Events[0].Body.ChannelID != googleChannelID("allowed-conversation") {
		t.Fatalf("filtered events = %#v", syncResponse.Events)
	}

	blockedChannel := googleChannelID("blocked-conversation")
	response = request(t, handler, http.MethodGet, "/v1/channels/"+blockedChannel+"/messages", nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("blocked history status = %d, want 404", response.Code)
	}
	body := []byte(`{"v":1,"kind":"command","type":"message.send","command_id":"blocked-send","body":{"client_message_id":"blocked-client","channel_id":"` + blockedChannel + `","text":"must not send"}}`)
	response = request(t, handler, http.MethodPost, "/v1/messages", body)
	if response.Code != http.StatusNotFound {
		t.Fatalf("blocked send status = %d, want 404", response.Code)
	}
	if err := server.importGoogleMessage(&gmproto.Message{
		MessageID:      "new-blocked-message",
		ConversationID: "blocked-conversation",
		MessageStatus:  &gmproto.MessageStatus{Status: gmproto.MessageStatusType_INCOMING_COMPLETE},
		MessageInfo: []*gmproto.MessageInfo{{
			Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "hidden"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if len(server.state.Messages) != 2 {
		t.Fatalf("blocked live message was stored; messages=%d", len(server.state.Messages))
	}
}

func TestGoogleWhitelistEmptyAndOmittedSemantics(t *testing.T) {
	server, handler := newTestServer(t)
	conversation := googleTestConversation("conversation", "Contact", "+15551234567")
	if err := server.upsertGoogleConversation(conversation); err != nil {
		t.Fatal(err)
	}

	server.google = &GoogleMessages{config: GoogleMessagesConfig{Whitelist: []string{}}}
	if err := server.applyGoogleWhitelist(); err != nil {
		t.Fatal(err)
	}
	response := request(t, handler, http.MethodGet, "/v1/bootstrap", nil)
	if bytes.Contains(response.Body.Bytes(), []byte(googleChannelID("conversation"))) {
		t.Fatalf("explicit empty whitelist exposed Google channel: %s", response.Body.String())
	}

	server.google.config.Whitelist = nil
	if err := server.applyGoogleWhitelist(); err != nil {
		t.Fatal(err)
	}
	response = request(t, handler, http.MethodGet, "/v1/bootstrap", nil)
	if !bytes.Contains(response.Body.Bytes(), []byte(googleChannelID("conversation"))) {
		t.Fatalf("omitted whitelist did not preserve unfiltered behavior: %s", response.Body.String())
	}

	server.google.config.Whitelist = []string{"conversation"}
	if !server.google.allowsConversation(conversation) {
		t.Fatal("conversation ID whitelist did not match")
	}
}

func googleTestConversation(id, name, number string) *gmproto.Conversation {
	return &gmproto.Conversation{
		ConversationID: id,
		Name:           name,
		Participants: []*gmproto.Participant{
			{ID: &gmproto.SmallInfo{Number: "+15550000000"}, IsMe: true},
			{ID: &gmproto.SmallInfo{Number: number}, IsVisible: true},
		},
	}
}

func googleTestMessage(conversationID string) *gmproto.Message {
	return &gmproto.Message{
		MessageID:      "message-" + conversationID,
		ConversationID: conversationID,
		MessageStatus:  &gmproto.MessageStatus{Status: gmproto.MessageStatusType_INCOMING_COMPLETE},
		MessageInfo: []*gmproto.MessageInfo{{
			Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "hello"}},
		}},
	}
}

func TestGoogleCookiesFromCopiedCurl(t *testing.T) {
	command := `curl 'https://messages.google.com/web/config' \
  -H 'accept: application/json' \
  -H 'cookie: SID=sid-value; HSID=hsid-value; SSID=ssid-value; OSID=osid-value; APISID=apisid=value; SAPISID=sapisid-value; __Secure-1PSIDTS=optional'`
	cookies, err := cookiesFromCurl(command)
	if err != nil {
		t.Fatal(err)
	}
	if cookies["SID"] != "sid-value" || cookies["APISID"] != "apisid=value" || cookies["SAPISID"] != "sapisid-value" {
		t.Fatalf("unexpected parsed cookies: %#v", cookies)
	}
}

func TestGoogleCookiesRequireDocumentedValues(t *testing.T) {
	_, err := cookiesFromCurl(`curl -H 'cookie: SID=only-one' https://messages.google.com/web/config`)
	if err == nil || !strings.Contains(err.Error(), "HSID") {
		t.Fatalf("error = %v, want missing required cookies", err)
	}
}

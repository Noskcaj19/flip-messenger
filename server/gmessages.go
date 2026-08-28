package flipmessenger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type googleConversation struct {
	ConversationID      string `json:"conversation_id"`
	Name                string `json:"name"`
	Transport           string `json:"transport"`
	OutgoingParticipant string `json:"outgoing_participant"`
	ParticipantNumbers  string `json:"participant_numbers,omitempty"`
	Allowed             bool   `json:"allowed"`
	SIMPresent          bool   `json:"sim_present"`
	SIMNumber           int32  `json:"sim_number"`
	SIMTwo              int32  `json:"sim_two"`
}

func (c googleConversation) Channel() Channel {
	if c.ConversationID == "" {
		return Channel{}
	}
	name := c.Name
	if name == "" {
		name = "Unknown conversation"
	}
	transport := strings.ToUpper(c.Transport)
	if transport == "" {
		transport = "SMS"
	}
	return Channel{
		ID:            googleChannelID(c.ConversationID),
		QualifiedName: c.ConversationID + ":" + strings.ToLower(transport),
		DisplayName:   name + " (" + transport + ")",
	}
}

type GoogleMessages struct {
	mu            sync.Mutex
	config        GoogleMessagesConfig
	server        *Server
	auth          *libgm.AuthData
	client        *libgm.Client
	cancel        context.CancelFunc
	pairing       bool
	pairingEmoji  string
	pairingCancel context.CancelFunc
	lastError     string
	closed        bool
	eventMu       sync.Mutex
	eventQueue    []any
	eventWake     chan struct{}
}

type googleOutboxWake struct{}
type googleSyncWake struct{}

func NewGoogleMessages(config GoogleMessagesConfig, server *Server) (*GoogleMessages, error) {
	auth := libgm.NewAuthData()
	contents, err := os.ReadFile(config.SessionFile)
	if err == nil {
		if err := json.Unmarshal(contents, auth); err != nil {
			return nil, fmt.Errorf("parse Google Messages session: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read Google Messages session: %w", err)
	}
	g := &GoogleMessages{
		config:    config,
		server:    server,
		auth:      auth,
		eventWake: make(chan struct{}, 1),
	}
	g.client = g.newClient(auth)
	return g, nil
}

func (g *GoogleMessages) newClient(auth *libgm.AuthData) *libgm.Client {
	// Keep the reverse-engineered protocol library at info level: some of its
	// debug events contain raw pairing structures. Our own debug logs expose
	// lifecycle and request metadata without credentials or message bodies.
	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel).With().Timestamp().Str("backend", "gmessages").Logger()
	client := libgm.NewClient(auth, nil, logger)
	client.SetEventHandler(g.enqueue)
	return client
}

func (g *GoogleMessages) Start(parent context.Context) error {
	g.mu.Lock()
	g.closed = false
	ctx, cancel := context.WithCancel(parent)
	g.cancel = cancel
	loggedIn := g.isPairedLocked()
	g.mu.Unlock()
	go g.runEvents(ctx)
	g.server.debugf("starting Google Messages backend paired=%t", loggedIn)
	if !loggedIn {
		return nil
	}
	if err := g.client.Connect(); err != nil {
		g.setError(err)
		// Keep the HTTP server available so the operator can inspect status and
		// reset an expired or revoked session.
		return nil
	}
	g.server.debugf("Google Messages connected; scheduling initial sync")
	g.enqueue(googleSyncWake{})
	return nil
}

func (g *GoogleMessages) Close() {
	g.mu.Lock()
	g.closed = true
	paired := g.isPairedLocked()
	if g.cancel != nil {
		g.cancel()
	}
	if g.pairingCancel != nil {
		g.pairingCancel()
	}
	g.mu.Unlock()
	g.client.Disconnect()
	if paired {
		if err := g.saveSession(); err != nil {
			log.Printf("save Google Messages session: %v", err)
		}
	}
}

func (g *GoogleMessages) enqueue(event any) {
	g.eventMu.Lock()
	g.eventQueue = append(g.eventQueue, event)
	g.eventMu.Unlock()
	select {
	case g.eventWake <- struct{}{}:
	default:
	}
}

func (g *GoogleMessages) runEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.eventWake:
			for {
				g.eventMu.Lock()
				if len(g.eventQueue) == 0 {
					g.eventMu.Unlock()
					break
				}
				event := g.eventQueue[0]
				g.eventQueue[0] = nil
				g.eventQueue = g.eventQueue[1:]
				g.eventMu.Unlock()
				g.handleEvent(ctx, event)
			}
		}
	}
}

func (g *GoogleMessages) handleEvent(ctx context.Context, raw any) {
	g.server.debugf("Google Messages event type=%T", raw)
	switch event := raw.(type) {
	case *events.AuthTokenRefreshed:
		if err := g.saveSession(); err != nil {
			g.setError(err)
		}
	case *events.ClientReady:
		g.setError(nil)
		g.syncConversations(ctx, event.Conversations)
		g.flushOutbox(ctx)
	case *gmproto.Conversation:
		if err := g.server.upsertGoogleConversation(event); err != nil {
			g.setError(err)
		}
	case *libgm.WrappedMessage:
		if err := g.server.importGoogleMessage(event.Message); err != nil {
			g.setError(err)
		}
	case *events.ListenFatalError:
		g.setError(event.Error)
	case *events.ListenTemporaryError:
		g.setError(event.Error)
	case *events.ListenRecovered:
		g.setError(nil)
	case *events.GaiaLoggedOut:
		g.setError(errors.New("Google Messages session was logged out"))
	case googleOutboxWake:
		g.flushOutbox(ctx)
	case googleSyncWake:
		g.syncConversations(ctx, nil)
		g.flushOutbox(ctx)
	}
}

func (g *GoogleMessages) WakeOutbox() {
	g.enqueue(googleOutboxWake{})
}

func (g *GoogleMessages) flushOutbox(ctx context.Context) {
	entries := g.server.googleOutbox()
	g.server.debugf("Google Messages outbox flush entries=%d", len(entries))
	for _, queued := range entries {
		commandID, entry := queued.commandID, queued.record
		conversation, found := g.server.googleConversation(entry.ChannelID)
		if !found {
			g.setError(fmt.Errorf("Google Messages outbox channel %s no longer exists", entry.ChannelID))
			continue
		}
		sendCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
		g.server.debugf("Google Messages outbox send command=%s channel=%s sequence=%d", commandID, entry.ChannelID, entry.Sequence)
		err := g.Send(sendCtx, conversation, entry.ClientMessageID, entry.Text)
		cancel()
		if err != nil {
			g.setError(err)
			time.AfterFunc(30*time.Second, g.WakeOutbox)
			return
		}
		if err := g.server.completeGoogleOutbox(commandID); err != nil {
			g.setError(err)
			return
		}
		g.server.debugf("Google Messages outbox delivered command=%s", commandID)
	}
}

func (g *GoogleMessages) syncConversations(ctx context.Context, initial []*gmproto.Conversation) {
	g.server.debugf("Google Messages conversation sync starting")
	conversations := initial
	response, err := g.client.ListConversations(ctx, 500, gmproto.ListConversationsRequest_INBOX)
	if err != nil {
		g.setError(fmt.Errorf("list Google Messages conversations: %w", err))
	} else {
		conversations = response.GetConversations()
	}
	g.server.debugf("Google Messages conversation sync found=%d history_per_conversation=%d", len(conversations), g.config.History)
	for _, conversation := range conversations {
		if err := g.server.upsertGoogleConversation(conversation); err != nil {
			g.setError(err)
			continue
		}
		if !g.allowsConversation(conversation) {
			continue
		}
		if g.config.History == 0 {
			continue
		}
		history, err := g.client.FetchMessages(ctx, conversation.GetConversationID(), int64(g.config.History), nil)
		if err != nil {
			g.setError(fmt.Errorf("fetch Google Messages history: %w", err))
			continue
		}
		messages := history.GetMessages()
		g.server.debugf("Google Messages history channel=%s messages=%d", googleChannelID(conversation.GetConversationID()), len(messages))
		sort.Slice(messages, func(i, j int) bool { return messages[i].GetTimestamp() < messages[j].GetTimestamp() })
		for _, message := range messages {
			if err := g.server.importGoogleMessage(message); err != nil {
				g.setError(err)
			}
		}
	}
}

func (g *GoogleMessages) Send(ctx context.Context, conversation googleConversation, transactionID, text string) error {
	if !g.client.IsLoggedIn() {
		return errors.New("Google Messages is not paired")
	}
	request := &gmproto.SendMessageRequest{
		ConversationID: conversation.ConversationID,
		TmpID:          transactionID,
		MessagePayload: &gmproto.MessagePayload{
			TmpID:          transactionID,
			TmpID2:         transactionID,
			ConversationID: conversation.ConversationID,
			ParticipantID:  conversation.OutgoingParticipant,
			MessageInfo: []*gmproto.MessageInfo{{
				Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: text}},
			}},
		},
	}
	if conversation.SIMPresent {
		request.SIMPayload = &gmproto.SIMPayload{
			SIMNumber: conversation.SIMNumber,
			Two:       conversation.SIMTwo,
		}
	}
	response, err := g.client.SendMessage(ctx, request)
	if err != nil {
		return fmt.Errorf("Google Messages send: %w", err)
	}
	if response.GetStatus() != gmproto.SendMessageResponse_SUCCESS {
		return fmt.Errorf("Google Messages rejected send: %s", response.GetStatus())
	}
	return nil
}

func (g *GoogleMessages) StartPairing(ctx context.Context, cookies map[string]string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pairing {
		return "", errors.New("Google Messages pairing is already in progress")
	}
	if g.isPairedLocked() {
		return "", errors.New("Google Messages is already paired")
	}
	if err := validateGoogleCookies(cookies); err != nil {
		return "", err
	}
	g.client.Disconnect()
	g.auth = libgm.NewAuthData()
	g.auth.SetCookies(cookies)
	g.client = g.newClient(g.auth)
	g.server.debugf("Google Messages manual pairing starting")
	if err := g.client.FetchConfig(ctx); err != nil {
		g.lastError = err.Error()
		return "", err
	}
	emoji, session, err := g.client.StartGaiaPairing(ctx)
	if err != nil {
		g.lastError = err.Error()
		return "", err
	}
	pairingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	g.pairing = true
	g.pairingEmoji = emoji
	g.pairingCancel = cancel
	g.lastError = ""
	g.server.debugf("Google Messages waiting for emoji confirmation emoji=%s", emoji)
	client := g.client
	go g.finishPairing(pairingCtx, client, session)
	return emoji, nil
}

func (g *GoogleMessages) finishPairing(ctx context.Context, client *libgm.Client, session *libgm.PairingSession) {
	_, err := client.FinishGaiaPairing(ctx, session)
	g.mu.Lock()
	if client != g.client || g.closed {
		g.mu.Unlock()
		client.Disconnect()
		return
	}
	g.pairing = false
	g.pairingEmoji = ""
	if g.pairingCancel != nil {
		g.pairingCancel()
	}
	g.pairingCancel = nil
	if err != nil {
		g.lastError = err.Error()
		g.mu.Unlock()
		client.Disconnect()
		log.Printf("Google Messages pairing failed: %v", err)
		return
	}
	g.lastError = ""
	g.mu.Unlock()
	g.server.debugf("Google Messages pairing confirmed")
	if err := g.saveSession(); err != nil {
		g.setError(fmt.Errorf("save Google Messages session: %w", err))
		return
	}
	if err := client.Reconnect(); err != nil {
		g.setError(fmt.Errorf("connect paired Google Messages session: %w", err))
		return
	}
	g.enqueue(googleSyncWake{})
}

func (g *GoogleMessages) ResetSession(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pairingCancel != nil {
		g.pairingCancel()
	}
	if g.isPairedLocked() {
		var err error
		if g.auth.IsGoogleAccount() {
			err = g.client.UnpairGaia(ctx)
		} else {
			_, err = g.client.UnpairBugle()
		}
		if err != nil {
			log.Printf("Google Messages remote unpair failed; clearing local session: %v", err)
		}
	}
	g.client.Disconnect()
	g.auth = libgm.NewAuthData()
	g.client = g.newClient(g.auth)
	g.pairing = false
	g.pairingEmoji = ""
	g.pairingCancel = nil
	g.lastError = ""
	g.server.debugf("Google Messages session reset")
	if err := os.Remove(g.config.SessionFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (g *GoogleMessages) status() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return map[string]any{
		"enabled":   true,
		"paired":    g.isPairedLocked(),
		"connected": g.client.IsConnected(),
		"pairing":   g.pairing,
		"emoji":     g.pairingEmoji,
		"error":     g.lastError,
	}
}

func (g *GoogleMessages) isPairedLocked() bool {
	return !g.pairing && g.client.IsLoggedIn() && g.auth.PairingID != uuid.Nil
}

func (g *GoogleMessages) setError(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err == nil {
		g.lastError = ""
	} else {
		g.lastError = err.Error()
		log.Printf("Google Messages: %v", err)
	}
}

func (g *GoogleMessages) saveSession() error {
	if err := os.MkdirAll(filepath.Dir(g.config.SessionFile), 0700); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(g.auth, "", "  ")
	if err != nil {
		return err
	}
	temporary := g.config.SessionFile + ".tmp"
	if err := os.WriteFile(temporary, contents, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, g.config.SessionFile)
}

func (s *Server) googleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if s.google == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, s.google.status())
}

func (s *Server) googlePair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if s.google == nil {
		writeError(w, http.StatusNotFound, "backend_disabled", "Google Messages is not enabled")
		return
	}
	cookies, err := readGoogleCookies(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_cookies", err.Error())
		return
	}
	emoji, err := s.google.StartPairing(r.Context(), cookies)
	if err != nil {
		writeError(w, http.StatusBadGateway, "pairing_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":          "waiting_for_emoji",
		"emoji":           emoji,
		"emoji_image_url": libgm.GetEmojiSVG(emoji),
		"expires_in":      180,
	})
}

type googleCookieRequest struct {
	Cookies map[string]string `json:"cookies"`
	Curl    string            `json:"curl"`
}

var requiredGoogleCookies = []string{"SID", "HSID", "OSID", "SSID", "APISID", "SAPISID"}

func readGoogleCookies(r *http.Request) (map[string]string, error) {
	contents, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes))
	if err != nil {
		return nil, errors.New("failed to read authentication data")
	}
	if strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "text/plain") {
		return cookiesFromCurl(string(contents))
	}
	var request googleCookieRequest
	if err := json.Unmarshal(contents, &request); err == nil {
		if request.Curl != "" {
			return cookiesFromCurl(request.Curl)
		}
		if request.Cookies != nil {
			if err := validateGoogleCookies(request.Cookies); err != nil {
				return nil, err
			}
			return request.Cookies, nil
		}
	}
	var cookies map[string]string
	if err := json.Unmarshal(contents, &cookies); err == nil {
		if err := validateGoogleCookies(cookies); err != nil {
			return nil, err
		}
		return cookies, nil
	}
	return nil, errors.New("expected a cookie JSON object, {\"cookies\":{...}}, or {\"curl\":\"curl ...\"}")
}

func validateGoogleCookies(cookies map[string]string) error {
	var missing []string
	for _, name := range requiredGoogleCookies {
		if strings.TrimSpace(cookies[name]) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required Google cookies: %s", strings.Join(missing, ", "))
	}
	return nil
}

func cookiesFromCurl(command string) (map[string]string, error) {
	arguments, err := splitCommandLine(command)
	if err != nil {
		return nil, err
	}
	var cookieHeader string
	for i := 0; i < len(arguments); i++ {
		argument := arguments[i]
		var value string
		switch {
		case argument == "-b" || argument == "--cookie" || argument == "-H" || argument == "--header":
			if i+1 < len(arguments) {
				i++
				value = arguments[i]
			}
		case strings.HasPrefix(argument, "--cookie="):
			value = strings.TrimPrefix(argument, "--cookie=")
		case strings.HasPrefix(argument, "--header="):
			value = strings.TrimPrefix(argument, "--header=")
		}
		value = strings.TrimPrefix(value, "$")
		if strings.HasPrefix(strings.ToLower(value), "cookie:") {
			value = strings.TrimSpace(value[len("cookie:"):])
		}
		if strings.Contains(value, "SID=") || strings.Contains(value, "SAPISID=") {
			cookieHeader = value
		}
	}
	if cookieHeader == "" {
		return nil, errors.New("copied cURL command does not contain a Cookie header")
	}
	cookies := make(map[string]string)
	for _, item := range strings.Split(cookieHeader, ";") {
		name, value, found := strings.Cut(strings.TrimSpace(item), "=")
		if found && name != "" {
			cookies[name] = value
		}
	}
	if err := validateGoogleCookies(cookies); err != nil {
		return nil, err
	}
	return cookies, nil
}

func splitCommandLine(command string) ([]string, error) {
	var arguments []string
	var current strings.Builder
	var quote rune
	escaped := false
	flush := func() {
		if current.Len() > 0 {
			arguments = append(arguments, current.String())
			current.Reset()
		}
	}
	for _, char := range command {
		if escaped {
			current.WriteRune(char)
			escaped = false
			continue
		}
		if char == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if char == quote {
				quote = 0
			} else {
				current.WriteRune(char)
			}
			continue
		}
		if char == '\'' || char == '"' {
			quote = char
		} else if char == ' ' || char == '\n' || char == '\r' || char == '\t' {
			flush()
		} else {
			current.WriteRune(char)
		}
	}
	if escaped || quote != 0 {
		return nil, errors.New("copied cURL command has unfinished quoting")
	}
	flush()
	return arguments, nil
}

func (s *Server) googleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}
	if s.google == nil {
		writeError(w, http.StatusNotFound, "backend_disabled", "Google Messages is not enabled")
		return
	}
	if err := s.google.ResetSession(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, "session_reset_failed", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) upsertGoogleConversation(conversation *gmproto.Conversation) error {
	if conversation == nil || conversation.GetConversationID() == "" {
		return nil
	}
	transport := "sms"
	if conversation.GetType() == gmproto.ConversationType_RCS {
		transport = "rcs"
	}
	name := strings.TrimSpace(conversation.GetName())
	participantNumbers := make([]string, 0)
	if name == "" {
		for _, participant := range conversation.GetParticipants() {
			if participant.GetIsMe() || !participant.GetIsVisible() {
				continue
			}
			name = firstNonempty(participant.GetFullName(), participant.GetFirstName(), participant.GetFormattedNumber(), participant.GetID().GetNumber())
			if name != "" {
				break
			}
		}
	}
	for _, participant := range conversation.GetParticipants() {
		if participant.GetIsMe() {
			continue
		}
		if number := normalizePhone(participant.GetID().GetNumber()); number != "" {
			participantNumbers = append(participantNumbers, number)
		}
	}
	sort.Strings(participantNumbers)
	participantNumbers = compactStrings(participantNumbers)
	payload := conversation.GetSimCard().GetSIMData().GetSIMPayload()
	record := googleConversation{
		ConversationID:      conversation.GetConversationID(),
		Name:                name,
		Transport:           transport,
		OutgoingParticipant: conversation.GetDefaultOutgoingID(),
		ParticipantNumbers:  strings.Join(participantNumbers, ","),
		Allowed:             s.google == nil || s.google.allowsConversation(conversation),
		SIMPresent:          payload != nil,
		SIMNumber:           payload.GetSIMNumber(),
		SIMTwo:              payload.GetTwo(),
	}
	channelID := googleChannelID(record.ConversationID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.GoogleConversations[channelID] == record {
		return nil
	}
	previous, existed := s.state.GoogleConversations[channelID]
	s.state.GoogleConversations[channelID] = record
	if err := s.saveState(); err != nil {
		if existed {
			s.state.GoogleConversations[channelID] = previous
		} else {
			delete(s.state.GoogleConversations, channelID)
		}
		return err
	}
	s.debugf("Google Messages conversation upserted channel=%s transport=%s", channelID, transport)
	return nil
}

func (s *Server) importGoogleMessage(remote *gmproto.Message) error {
	if remote == nil || remote.GetMessageID() == "" || remote.GetConversationID() == "" {
		return nil
	}
	status := remote.GetMessageStatus().GetStatus()
	if status <= 0 || status >= 200 {
		return nil
	}
	var parts []string
	if subject := strings.TrimSpace(remote.GetSubject()); subject != "" {
		parts = append(parts, subject)
	}
	for _, info := range remote.GetMessageInfo() {
		if text := strings.TrimSpace(info.GetMessageContent().GetContent()); text != "" {
			parts = append(parts, text)
		} else if media := info.GetMediaContent(); media != nil {
			name := firstNonempty(media.GetMediaName(), media.GetMimeType(), "attachment")
			parts = append(parts, "[Attachment: "+name+"]")
		}
	}
	if len(parts) == 0 {
		return nil
	}
	channelID := googleChannelID(remote.GetConversationID())
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.state.GoogleMessages[remote.GetMessageID()]; exists {
		return nil
	}
	if conversation, exists := s.state.GoogleConversations[channelID]; exists {
		if !conversation.Allowed {
			return nil
		}
	} else if s.google != nil && s.google.whitelistEnabled() {
		// A message event alone does not include enough participant information
		// to safely evaluate a phone-number allowlist. Wait for conversation data.
		return nil
	}
	if remote.GetTmpID() != "" {
		for _, message := range s.state.Messages {
			if message.ClientMessageID == remote.GetTmpID() {
				s.state.GoogleMessages[remote.GetMessageID()] = message.ID
				removed := make(map[string]outboxRecord)
				for commandID, entry := range s.state.Outbox {
					if entry.ClientMessageID == remote.GetTmpID() {
						removed[commandID] = entry
						delete(s.state.Outbox, commandID)
					}
				}
				if err := s.saveState(); err != nil {
					delete(s.state.GoogleMessages, remote.GetMessageID())
					for commandID, entry := range removed {
						s.state.Outbox[commandID] = entry
					}
					return err
				}
				return nil
			}
		}
	}
	addedConversation := false
	if _, exists := s.state.GoogleConversations[channelID]; !exists {
		s.state.GoogleConversations[channelID] = googleConversation{
			ConversationID: remote.GetConversationID(),
			Name:           "Unknown conversation",
			Transport:      "sms",
			Allowed:        true,
		}
		addedConversation = true
	}
	author := "channel"
	if status > 0 && status < 100 {
		author = "self"
	}
	createdAt := s.now().UTC()
	if remote.GetTimestamp() > 0 {
		createdAt = time.UnixMicro(remote.GetTimestamp()).UTC()
	}
	message := Message{
		ID:        "msg_gm_" + shortHash(remote.GetMessageID()),
		ChannelID: channelID,
		Author:    author,
		Text:      strings.Join(parts, "\n"),
		CreatedAt: createdAt.Format(time.RFC3339Nano),
	}
	previousMessageCount := len(s.state.Messages)
	previousEventCount := len(s.state.Events)
	previousCursor := s.state.NextCursor
	s.appendMessage(message)
	s.state.GoogleMessages[remote.GetMessageID()] = message.ID
	if err := s.saveState(); err != nil {
		s.state.Messages = s.state.Messages[:previousMessageCount]
		s.state.Events = s.state.Events[:previousEventCount]
		s.state.NextCursor = previousCursor
		delete(s.state.GoogleMessages, remote.GetMessageID())
		if addedConversation {
			delete(s.state.GoogleConversations, channelID)
		}
		return err
	}
	s.debugf("Google Messages message imported channel=%s message=%s author=%s", channelID, message.ID, author)
	return nil
}

type queuedOutbox struct {
	commandID string
	record    outboxRecord
}

func (s *Server) googleOutbox() []queuedOutbox {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]queuedOutbox, 0, len(s.state.Outbox))
	for commandID, entry := range s.state.Outbox {
		entries = append(entries, queuedOutbox{commandID: commandID, record: entry})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].record.Sequence < entries[j].record.Sequence })
	return entries
}

func (s *Server) googleConversation(channelID string) (googleConversation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conversation, found := s.state.GoogleConversations[channelID]
	return conversation, found && conversation.Allowed
}

func (g *GoogleMessages) whitelistEnabled() bool {
	return g.config.Whitelist != nil
}

func (g *GoogleMessages) allowsConversation(conversation *gmproto.Conversation) bool {
	if !g.whitelistEnabled() {
		return true
	}
	for _, allowed := range g.config.Whitelist {
		if allowed == conversation.GetConversationID() {
			return true
		}
		allowedPhone := normalizePhone(allowed)
		if allowedPhone == "" {
			continue
		}
		for _, participant := range conversation.GetParticipants() {
			if !participant.GetIsMe() && normalizePhone(participant.GetID().GetNumber()) == allowedPhone {
				return true
			}
		}
	}
	return false
}

func (g *GoogleMessages) allowsRecord(conversation googleConversation) bool {
	if !g.whitelistEnabled() {
		return true
	}
	phones := strings.Split(conversation.ParticipantNumbers, ",")
	for _, allowed := range g.config.Whitelist {
		if allowed == conversation.ConversationID {
			return true
		}
		allowedPhone := normalizePhone(allowed)
		for _, phone := range phones {
			if allowedPhone != "" && phone == allowedPhone {
				return true
			}
		}
	}
	return false
}

func (s *Server) applyGoogleWhitelist() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	previous := make(map[string]googleConversation)
	for channelID, conversation := range s.state.GoogleConversations {
		allowed := s.google.allowsRecord(conversation)
		if conversation.Allowed != allowed {
			previous[channelID] = conversation
			conversation.Allowed = allowed
			s.state.GoogleConversations[channelID] = conversation
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := s.saveState(); err != nil {
		for channelID, conversation := range previous {
			s.state.GoogleConversations[channelID] = conversation
		}
		return err
	}
	return nil
}

func normalizePhone(value string) string {
	var normalized strings.Builder
	for _, char := range value {
		if char >= '0' && char <= '9' {
			normalized.WriteRune(char)
		}
	}
	if normalized.Len() < 5 {
		return ""
	}
	return normalized.String()
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	compacted := values[:1]
	for _, value := range values[1:] {
		if value != compacted[len(compacted)-1] {
			compacted = append(compacted, value)
		}
	}
	return compacted
}

func (s *Server) completeGoogleOutbox(commandID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.state.Outbox[commandID]; !exists {
		return nil
	}
	entry := s.state.Outbox[commandID]
	delete(s.state.Outbox, commandID)
	if err := s.saveState(); err != nil {
		s.state.Outbox[commandID] = entry
		return err
	}
	return nil
}

func googleChannelID(conversationID string) string {
	return "gmessages_" + shortHash(conversationID)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:12])
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func sortChannels(channels []Channel) {
	sort.Slice(channels, func(i, j int) bool {
		return strings.ToLower(channels[i].DisplayName) < strings.ToLower(channels[j].DisplayName)
	})
}

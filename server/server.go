package flipmessenger

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxRequestBytes = 64 << 10

type Config struct {
	Listen         string               `json:"listen"`
	AllowHTTP      bool                 `json:"allow_http"`
	TLSCert        string               `json:"tls_cert"`
	TLSKey         string               `json:"tls_key"`
	DataFile       string               `json:"data_file"`
	APIToken       string               `json:"api_token"`
	Debug          bool                 `json:"debug"`
	Channels       []Channel            `json:"channels"`
	GoogleMessages GoogleMessagesConfig `json:"google_messages"`
}

type GoogleMessagesConfig struct {
	Enabled     bool     `json:"enabled"`
	SessionFile string   `json:"session_file"`
	History     int      `json:"history"`
	Whitelist   []string `json:"whitelist"`
}

type Channel struct {
	ID            string `json:"channel_id"`
	QualifiedName string `json:"qualified_name"`
	DisplayName   string `json:"display_name"`
	EchoPrefix    string `json:"-"`
}

func (c *Channel) UnmarshalJSON(data []byte) error {
	type channelJSON struct {
		ID            string `json:"id"`
		QualifiedName string `json:"qualified_name"`
		DisplayName   string `json:"display_name"`
		EchoPrefix    string `json:"echo_prefix"`
	}
	var value channelJSON
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	c.ID = value.ID
	c.QualifiedName = value.QualifiedName
	c.DisplayName = value.DisplayName
	c.EchoPrefix = value.EchoPrefix
	return nil
}

type Message struct {
	ID              string `json:"message_id"`
	ClientMessageID string `json:"client_message_id,omitempty"`
	ChannelID       string `json:"channel_id"`
	Author          string `json:"author"`
	Text            string `json:"text"`
	CreatedAt       string `json:"created_at"`
}

type Event struct {
	Version    int     `json:"v"`
	Kind       string  `json:"kind"`
	Type       string  `json:"type"`
	ID         string  `json:"event_id"`
	Cursor     string  `json:"cursor"`
	OccurredAt string  `json:"occurred_at"`
	Body       Message `json:"body"`
}

type commandRecord struct {
	BodyHash  string `json:"body_hash"`
	MessageID string `json:"message_id"`
	Cursor    string `json:"cursor"`
}

type outboxRecord struct {
	ChannelID       string `json:"channel_id"`
	ClientMessageID string `json:"client_message_id"`
	Text            string `json:"text"`
	Sequence        uint64 `json:"sequence"`
}

type state struct {
	NextCursor          uint64                        `json:"next_cursor"`
	Messages            []Message                     `json:"messages"`
	Events              []Event                       `json:"events"`
	Commands            map[string]commandRecord      `json:"commands"`
	Outbox              map[string]outboxRecord       `json:"outbox,omitempty"`
	GoogleConversations map[string]googleConversation `json:"google_conversations,omitempty"`
	GoogleMessages      map[string]string             `json:"google_messages,omitempty"`
}

type Server struct {
	mu       sync.Mutex
	sendMu   sync.Mutex
	config   Config
	channels map[string]Channel
	state    state
	now      func() time.Time
	google   *GoogleMessages
}

func LoadConfig(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(contents, &config); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if config.Listen == "" {
		config.Listen = ":8443"
	}
	if config.DataFile == "" {
		config.DataFile = "data/state.json"
	}
	if len(config.APIToken) < 16 {
		return Config{}, errors.New("api_token must be at least 16 characters")
	}
	if len(config.Channels) == 0 && !config.GoogleMessages.Enabled {
		return Config{}, errors.New("at least one channel or google_messages must be enabled")
	}
	if config.GoogleMessages.Enabled {
		if config.GoogleMessages.SessionFile == "" {
			config.GoogleMessages.SessionFile = "data/gmessages-session.json"
		}
		if config.GoogleMessages.History == 0 {
			config.GoogleMessages.History = 50
		}
		if config.GoogleMessages.History < 0 || config.GoogleMessages.History > 500 {
			return Config{}, errors.New("google_messages.history must be between 0 and 500")
		}
	}
	seen := make(map[string]bool)
	for _, channel := range config.Channels {
		if channel.ID == "" || channel.QualifiedName == "" || channel.DisplayName == "" {
			return Config{}, errors.New("every channel needs id, qualified_name, and display_name")
		}
		if seen[channel.ID] {
			return Config{}, fmt.Errorf("duplicate channel id %q", channel.ID)
		}
		seen[channel.ID] = true
	}
	return config, nil
}

func New(config Config) (*Server, error) {
	server := &Server{
		config:   config,
		channels: make(map[string]Channel),
		now:      time.Now,
		state: state{
			Messages:            make([]Message, 0),
			Events:              make([]Event, 0),
			Commands:            make(map[string]commandRecord),
			Outbox:              make(map[string]outboxRecord),
			GoogleConversations: make(map[string]googleConversation),
			GoogleMessages:      make(map[string]string),
		},
	}
	for _, channel := range config.Channels {
		server.channels[channel.ID] = channel
	}
	if err := server.loadState(); err != nil {
		return nil, err
	}
	if config.GoogleMessages.Enabled {
		google, err := NewGoogleMessages(config.GoogleMessages, server)
		if err != nil {
			return nil, err
		}
		server.google = google
		if err := server.applyGoogleWhitelist(); err != nil {
			return nil, err
		}
	}
	server.debugf("loaded state: static_channels=%d google_channels=%d messages=%d events=%d outbox=%d",
		len(server.channels), len(server.state.GoogleConversations), len(server.state.Messages), len(server.state.Events), len(server.state.Outbox))
	return server, nil
}

func (s *Server) Start(ctx context.Context) error {
	if s.google == nil {
		return nil
	}
	return s.google.Start(ctx)
}

func (s *Server) Close() {
	if s.google != nil {
		s.google.Close()
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/v1/bootstrap", s.auth(s.bootstrap))
	mux.HandleFunc("/v1/sync", s.auth(s.syncEvents))
	mux.HandleFunc("/v1/messages", s.auth(s.messages))
	mux.HandleFunc("/v1/channels/", s.auth(s.channelMessages))
	mux.HandleFunc("/v1/google/status", s.auth(s.googleStatus))
	mux.HandleFunc("/v1/google/pair", s.auth(s.googlePair))
	mux.HandleFunc("/v1/google/session", s.auth(s.googleSession))
	return requestHeaders(s.requestLogging(mux))
}

type responseStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseStatusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseStatusWriter) Write(contents []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(contents)
}

func (s *Server) requestLogging(next http.Handler) http.Handler {
	if !s.config.Debug {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		tracked := &responseStatusWriter{ResponseWriter: w}
		next.ServeHTTP(tracked, r)
		status := tracked.status
		if status == 0 {
			status = http.StatusOK
		}
		s.debugf("http method=%s path=%s status=%d duration=%s", r.Method, r.URL.Path, status, time.Since(started).Round(time.Millisecond))
	})
}

func (s *Server) debugf(format string, values ...any) {
	if s.config.Debug {
		log.Printf("DEBUG: "+format, values...)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		provided := strings.TrimPrefix(header, "Bearer ")
		if !strings.HasPrefix(header, "Bearer ") ||
			len(provided) != len(s.config.APIToken) ||
			subtle.ConstantTimeCompare([]byte(provided), []byte(s.config.APIToken)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		next(w, r)
	}
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, http.StatusOK, struct {
		Version  int       `json:"v"`
		Cursor   string    `json:"cursor"`
		Channels []Channel `json:"channels"`
		Messages []Message `json:"messages"`
	}{1, strconv.FormatUint(s.state.NextCursor, 10), s.allChannels(), s.visibleMessages()})
}

func (s *Server) allChannels() []Channel {
	channels := make([]Channel, 0, len(s.config.Channels)+len(s.state.GoogleConversations))
	channels = append(channels, s.config.Channels...)
	google := make([]Channel, 0, len(s.state.GoogleConversations))
	for _, conversation := range s.state.GoogleConversations {
		if !conversation.Allowed {
			continue
		}
		google = append(google, conversation.Channel())
	}
	sortChannels(google)
	return append(channels, google...)
}

func (s *Server) syncEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	after, err := parseCursor(r.URL.Query().Get("after"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_cursor", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]Event, 0)
	for _, event := range s.state.Events {
		cursor, _ := strconv.ParseUint(event.Cursor, 10, 64)
		if cursor > after && s.channelVisible(event.Body.ChannelID) {
			events = append(events, event)
		}
		if len(events) == 200 {
			break
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Version       int       `json:"v"`
		Events        []Event   `json:"events"`
		HighWatermark string    `json:"high_watermark"`
		Channels      []Channel `json:"channels"`
	}{1, events, strconv.FormatUint(s.state.NextCursor, 10), s.allChannels()})
	s.debugf("sync after=%d returned_events=%d high_watermark=%d", after, len(events), s.state.NextCursor)
}

type sendCommand struct {
	Version   int    `json:"v"`
	Kind      string `json:"kind"`
	Type      string `json:"type"`
	CommandID string `json:"command_id"`
	Body      struct {
		ClientMessageID string `json:"client_message_id"`
		ChannelID       string `json:"channel_id"`
		Text            string `json:"text"`
	} `json:"body"`
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var command sendCommand
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if command.Version != 1 || command.Kind != "command" || command.Type != "message.send" {
		writeError(w, http.StatusBadRequest, "unsupported_command", "expected v1 message.send command")
		return
	}
	if command.CommandID == "" || command.Body.ClientMessageID == "" {
		writeError(w, http.StatusBadRequest, "validation_failed", "command_id and client_message_id are required")
		return
	}
	text := strings.TrimSpace(command.Body.Text)
	if text == "" || len([]rune(text)) > 2000 {
		writeError(w, http.StatusBadRequest, "validation_failed", "text must contain 1 to 2000 characters")
		return
	}
	channel, found := s.channel(command.Body.ChannelID)
	if !found {
		writeError(w, http.StatusNotFound, "channel_not_found", "unknown channel")
		return
	}

	hash := commandHash(command.Body.ChannelID, command.Body.ClientMessageID, text)
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if previous, exists := s.state.Commands[command.CommandID]; exists {
		if previous.BodyHash != hash {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, "idempotency_conflict", "command_id was already used with a different body")
			return
		}
		s.mu.Unlock()
		writeAck(w, command.CommandID, previous.MessageID, previous.Cursor, true)
		return
	}
	_, isGoogle := s.state.GoogleConversations[channel.ID]
	if isGoogle {
		if s.google == nil {
			s.mu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "backend_unavailable", "Google Messages backend is disabled")
			return
		}
	}
	defer s.mu.Unlock()

	message := Message{
		ID:              newID("msg"),
		ClientMessageID: command.Body.ClientMessageID,
		ChannelID:       channel.ID,
		Author:          "self",
		Text:            text,
		CreatedAt:       s.now().UTC().Format(time.RFC3339Nano),
	}
	previousMessageCount := len(s.state.Messages)
	previousEventCount := len(s.state.Events)
	previousCursor := s.state.NextCursor
	s.appendMessage(message)
	if isGoogle {
		s.state.Outbox[command.CommandID] = outboxRecord{
			ChannelID:       channel.ID,
			ClientMessageID: command.Body.ClientMessageID,
			Text:            text,
			Sequence:        s.state.NextCursor,
		}
	}

	if !isGoogle {
		prefix := channel.EchoPrefix
		if prefix == "" {
			prefix = channel.DisplayName
		}
		echo := Message{
			ID:        newID("msg"),
			ChannelID: channel.ID,
			Author:    "channel",
			Text:      prefix + ": " + text,
			CreatedAt: s.now().UTC().Format(time.RFC3339Nano),
		}
		s.appendMessage(echo)
	}
	committedCursor := strconv.FormatUint(s.state.NextCursor, 10)
	s.state.Commands[command.CommandID] = commandRecord{hash, message.ID, committedCursor}
	if err := s.saveState(); err != nil {
		s.state.Messages = s.state.Messages[:previousMessageCount]
		s.state.Events = s.state.Events[:previousEventCount]
		s.state.NextCursor = previousCursor
		delete(s.state.Commands, command.CommandID)
		delete(s.state.Outbox, command.CommandID)
		writeError(w, http.StatusInternalServerError, "storage_failed", "message was not stored")
		return
	}
	writeAck(w, command.CommandID, message.ID, committedCursor, false)
	s.debugf("accepted message command channel=%s message=%s google=%t cursor=%s", channel.ID, message.ID, isGoogle, committedCursor)
	if isGoogle {
		s.google.WakeOutbox()
	}
}

func (s *Server) appendMessage(message Message) {
	s.state.NextCursor++
	cursor := strconv.FormatUint(s.state.NextCursor, 10)
	s.state.Messages = append(s.state.Messages, message)
	s.state.Events = append(s.state.Events, Event{
		Version:    1,
		Kind:       "event",
		Type:       "message.created",
		ID:         newID("evt"),
		Cursor:     cursor,
		OccurredAt: message.CreatedAt,
		Body:       message,
	})
}

func (s *Server) channel(id string) (Channel, bool) {
	if channel, found := s.channels[id]; found {
		return channel, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	conversation, found := s.state.GoogleConversations[id]
	return conversation.Channel(), found && conversation.Allowed
}

func (s *Server) channelMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/channels/"), "/")
	if len(parts) != 2 || parts[1] != "messages" {
		http.NotFound(w, r)
		return
	}
	_, staticFound := s.channels[parts[0]]
	s.mu.Lock()
	conversation, googleFound := s.state.GoogleConversations[parts[0]]
	googleFound = googleFound && conversation.Allowed
	s.mu.Unlock()
	if !staticFound && !googleFound {
		writeError(w, http.StatusNotFound, "channel_not_found", "unknown channel")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	messages := make([]Message, 0)
	for _, message := range s.state.Messages {
		if message.ChannelID == parts[0] {
			messages = append(messages, message)
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"v": 1, "messages": messages})
}

func (s *Server) visibleMessages() []Message {
	messages := make([]Message, 0, len(s.state.Messages))
	for _, message := range s.state.Messages {
		if s.channelVisible(message.ChannelID) {
			messages = append(messages, message)
		}
	}
	return messages
}

func (s *Server) channelVisible(channelID string) bool {
	conversation, google := s.state.GoogleConversations[channelID]
	if google {
		return conversation.Allowed
	}
	return !strings.HasPrefix(channelID, "gmessages_")
}

func (s *Server) loadState() error {
	contents, err := os.ReadFile(s.config.DataFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	if err := json.Unmarshal(contents, &s.state); err != nil {
		return fmt.Errorf("parse state: %w", err)
	}
	if s.state.Commands == nil {
		s.state.Commands = make(map[string]commandRecord)
	}
	if s.state.Outbox == nil {
		s.state.Outbox = make(map[string]outboxRecord)
	}
	if s.state.Messages == nil {
		s.state.Messages = make([]Message, 0)
	}
	if s.state.Events == nil {
		s.state.Events = make([]Event, 0)
	}
	if s.state.GoogleConversations == nil {
		s.state.GoogleConversations = make(map[string]googleConversation)
	}
	if s.state.GoogleMessages == nil {
		s.state.GoogleMessages = make(map[string]string)
	}
	return nil
}

func (s *Server) saveState() error {
	if err := os.MkdirAll(filepath.Dir(s.config.DataFile), 0700); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	temporary := s.config.DataFile + ".tmp"
	if err := os.WriteFile(temporary, contents, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, s.config.DataFile)
}

func commandHash(channelID, clientMessageID, text string) string {
	sum := sha256.Sum256([]byte(channelID + "\x00" + clientMessageID + "\x00" + text))
	return hex.EncodeToString(sum[:])
}

func newID(prefix string) string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(value)
}

func parseCursor(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("cursor must be a non-negative integer")
	}
	return cursor, nil
}

func writeAck(w http.ResponseWriter, commandID, messageID, cursor string, duplicate bool) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"v":                1,
		"kind":             "ack",
		"command_id":       commandID,
		"outcome":          "accepted",
		"committed_cursor": cursor,
		"duplicate":        duplicate,
		"result": map[string]string{
			"message_id": messageID,
		},
	})
}

func requestHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]interface{}{
		"v": 1,
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

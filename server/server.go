package loopback

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Listen    string    `json:"listen"`
	AllowHTTP bool      `json:"allow_http"`
	TLSCert   string    `json:"tls_cert"`
	TLSKey    string    `json:"tls_key"`
	DataFile  string    `json:"data_file"`
	APIToken  string    `json:"api_token"`
	Channels  []Channel `json:"channels"`
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

type state struct {
	NextCursor uint64                   `json:"next_cursor"`
	Messages   []Message                `json:"messages"`
	Events     []Event                  `json:"events"`
	Commands   map[string]commandRecord `json:"commands"`
}

type Server struct {
	mu       sync.Mutex
	config   Config
	channels map[string]Channel
	state    state
	now      func() time.Time
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
	if len(config.Channels) == 0 {
		return Config{}, errors.New("at least one channel is required")
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
			Messages: make([]Message, 0),
			Events:   make([]Event, 0),
			Commands: make(map[string]commandRecord),
		},
	}
	for _, channel := range config.Channels {
		server.channels[channel.ID] = channel
	}
	if err := server.loadState(); err != nil {
		return nil, err
	}
	return server, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/v1/bootstrap", s.auth(s.bootstrap))
	mux.HandleFunc("/v1/sync", s.auth(s.syncEvents))
	mux.HandleFunc("/v1/messages", s.auth(s.messages))
	mux.HandleFunc("/v1/channels/", s.auth(s.channelMessages))
	return requestHeaders(mux)
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
	}{1, strconv.FormatUint(s.state.NextCursor, 10), s.config.Channels, s.state.Messages})
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
		if cursor > after {
			events = append(events, event)
		}
		if len(events) == 200 {
			break
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Version       int     `json:"v"`
		Events        []Event `json:"events"`
		HighWatermark string  `json:"high_watermark"`
	}{1, events, strconv.FormatUint(s.state.NextCursor, 10)})
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
	channel, found := s.channels[command.Body.ChannelID]
	if !found {
		writeError(w, http.StatusNotFound, "channel_not_found", "unknown channel")
		return
	}

	hash := commandHash(command.Body.ChannelID, command.Body.ClientMessageID, text)
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.state.Commands[command.CommandID]; exists {
		if previous.BodyHash != hash {
			writeError(w, http.StatusConflict, "idempotency_conflict", "command_id was already used with a different body")
			return
		}
		writeAck(w, command.CommandID, previous.MessageID, previous.Cursor, true)
		return
	}

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
	committedCursor := strconv.FormatUint(s.state.NextCursor, 10)
	s.state.Commands[command.CommandID] = commandRecord{hash, message.ID, committedCursor}
	if err := s.saveState(); err != nil {
		s.state.Messages = s.state.Messages[:previousMessageCount]
		s.state.Events = s.state.Events[:previousEventCount]
		s.state.NextCursor = previousCursor
		delete(s.state.Commands, command.CommandID)
		writeError(w, http.StatusInternalServerError, "storage_failed", "message was not stored")
		return
	}
	writeAck(w, command.CommandID, message.ID, committedCursor, false)
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
	if _, found := s.channels[parts[0]]; !found {
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
	if s.state.Messages == nil {
		s.state.Messages = make([]Message, 0)
	}
	if s.state.Events == nil {
		s.state.Events = make([]Event, 0)
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

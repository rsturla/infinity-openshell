// Go agent loop against the Rust RAP tool server.
//
// The Go process owns the conversation loop and the callback listener. It does
// not create sandboxes; the Rust server owns the OpenShell lifecycle. The only
// contract is RAP over HTTP/JSON.
//
// Run the Rust server first:
//
//	cargo run --features cli --bin infinity-openshell -- serve \
//	  --gateway demo --base-url http://127.0.0.1:3001
//
// Then run this program:
//
//	cd examples/go-agent
//	go run .
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultRapURL = "http://127.0.0.1:3001"
	marker        = "go-agent-ok"
)

type invocation struct {
	Operation       string          `json:"operation"`
	Arguments       json.RawMessage `json:"arguments"`
	ID              string          `json:"id"`
	CallID          *string         `json:"call_id"`
	CallbackURL     string          `json:"callback_url"`
	GroupID         string          `json:"group_id"`
	UserID          *string         `json:"user_id,omitempty"`
	ThreadAncestors []string        `json:"thread_ancestors,omitempty"`
}

type callback struct {
	Type       string `json:"type"`
	GroupID    string `json:"group_id"`
	ID         string `json:"id"`
	ToolCallID string `json:"tool_call_id"`
	Text       string `json:"text"`
	Final      *bool  `json:"final"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	base := strings.TrimRight(env("RAP_URL", defaultRapURL), "/")
	token := os.Getenv("RAP_TOKEN")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	callbacks := make(chan callback, 16)

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		body, err := io.ReadAll(io.LimitReader(request.Body, 4<<20))
		if err != nil {
			http.Error(writer, "cannot read callback", http.StatusBadRequest)
			return
		}
		var message callback
		if err := json.Unmarshal(body, &message); err != nil {
			http.Error(writer, "cannot decode callback", http.StatusBadRequest)
			return
		}
		callbacks <- message
		writer.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = server.Serve(listener)
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	now := time.Now().UnixNano()
	callID := fmt.Sprintf("go-call-%d", now)
	threadID := fmt.Sprintf("go-thread-%d", now)
	call := invocation{
		Operation:   "openshell_exec",
		Arguments:   json.RawMessage(`{"command":["sh","-c","printf ` + marker + `"]}`),
		ID:          callID,
		CallbackURL: fmt.Sprintf("http://127.0.0.1:%d/callback", port),
		GroupID:     threadID,
	}
	body, err := json.Marshal(call)
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, base+"/invoke", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("content-type", "application/json")
	if token != "" {
		request.Header.Set("authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	ack, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("invoke returned %s: %s", response.Status, ack)
	}
	fmt.Printf("ack: %s\n", response.Status)

	deadline := time.After(2 * time.Minute)
	for {
		select {
		case message := <-callbacks:
			switch message.Type {
			case "subscription_event":
				final := message.Final != nil && *message.Final
				fmt.Printf("event final=%t: %s\n", final, message.Text)
			case "tool_result":
				if message.ID != callID {
					continue
				}
				fmt.Printf("result:\n%s\n", message.Text)
				closeThread(base, token, threadID)
				if !strings.Contains(message.Text, marker) {
					return fmt.Errorf("result did not contain %q", marker)
				}
				return nil
			}
		case <-deadline:
			return fmt.Errorf("timed out waiting for the tool result")
		}
	}
}

func closeThread(base, token, threadID string) {
	body, err := json.Marshal(map[string]string{"thread_id": threadID})
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPost, base+"/close_thread", bytes.NewReader(body))
	if err != nil {
		return
	}
	request.Header.Set("content-type", "application/json")
	if token != "" {
		request.Header.Set("authorization", "Bearer "+token)
	}
	response, err := (&http.Client{Timeout: 3 * time.Minute}).Do(request)
	if err != nil {
		fmt.Fprintln(os.Stderr, "close_thread:", err)
		return
	}
	defer response.Body.Close()
	fmt.Printf("close: %s\n", response.Status)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

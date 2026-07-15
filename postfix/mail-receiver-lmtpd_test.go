package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestDeliverDrainsResponseAndReusesConnection(t *testing.T) {
	var newConnections atomic.Int64
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"delivered","email_id":"00000000-0000-0000-0000-000000000001"}`))
	}))
	ts.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConnections.Add(1)
		}
	}
	ts.Start()
	defer ts.Close()

	client := newAPIHTTPClient()
	defer client.CloseIdleConnections()
	srv := &server{apiURL: ts.URL, httpClient: client}
	payload := deliverPayload{Recipient: "test@example.com", Raw: "hello"}
	for i := 0; i < 25; i++ {
		if err := srv.deliver(context.Background(), payload); err != nil {
			t.Fatalf("deliver %d: %v", i, err)
		}
	}

	if got := newConnections.Load(); got != 1 {
		t.Fatalf("expected one reused HTTP connection, got %d", got)
	}
}

func TestValidateBatchResults(t *testing.T) {
	recipients := []string{"a@example.com", "b@example.com"}
	tests := []struct {
		name    string
		results []batchResult
		wantErr bool
	}{
		{
			name: "valid",
			results: []batchResult{
				{Recipient: "a@example.com", Status: "delivered"},
				{Recipient: "b@example.com", Status: "discarded"},
			},
		},
		{
			name:    "missing result",
			results: []batchResult{{Recipient: "a@example.com", Status: "delivered"}},
			wantErr: true,
		},
		{
			name: "wrong order",
			results: []batchResult{
				{Recipient: "b@example.com", Status: "delivered"},
				{Recipient: "a@example.com", Status: "delivered"},
			},
			wantErr: true,
		},
		{
			name: "invalid status",
			results: []batchResult{
				{Recipient: "a@example.com", Status: "maybe"},
				{Recipient: "b@example.com", Status: "delivered"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBatchResults(recipients, tt.results)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateBatchResults() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

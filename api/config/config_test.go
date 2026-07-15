package config

import (
	"testing"
	"time"
)

func TestDeliveryBatchEnabled(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "default", value: "", want: true},
		{name: "true", value: "true", want: true},
		{name: "numeric true", value: "1", want: true},
		{name: "false", value: "false", want: false},
		{name: "invalid falls back", value: "invalid", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DELIVERY_BATCH_ENABLED", tt.value)
			if got := Load().DeliveryBatchEnabled; got != tt.want {
				t.Fatalf("DeliveryBatchEnabled=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeliveryBatchLimits(t *testing.T) {
	tests := []struct {
		name     string
		max      string
		wait     string
		wantMax  int
		wantWait time.Duration
	}{
		{name: "valid", max: "128", wait: "2ms", wantMax: 128, wantWait: 2 * time.Millisecond},
		{name: "oversized max", max: "1000000000", wait: "1ms", wantMax: 64, wantWait: time.Millisecond},
		{name: "too short wait", max: "64", wait: "1us", wantMax: 64, wantWait: time.Millisecond},
		{name: "too long wait", max: "64", wait: "1s", wantMax: 64, wantWait: time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DELIVERY_BATCH_MAX", tt.max)
			t.Setenv("DELIVERY_BATCH_WAIT", tt.wait)
			cfg := Load()
			if cfg.DeliveryBatchMax != tt.wantMax || cfg.DeliveryBatchWait != tt.wantWait {
				t.Fatalf("batch config=(%d,%s), want (%d,%s)",
					cfg.DeliveryBatchMax, cfg.DeliveryBatchWait, tt.wantMax, tt.wantWait)
			}
		})
	}
}

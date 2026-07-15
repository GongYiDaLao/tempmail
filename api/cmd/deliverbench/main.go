package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type singlePayload struct {
	Recipient string `json:"recipient"`
	Sender    string `json:"sender"`
	Subject   string `json:"subject"`
	BodyText  string `json:"body_text"`
	BodyHTML  string `json:"body_html"`
	Raw       string `json:"raw"`
}

type batchPayload struct {
	Recipients []string `json:"recipients"`
	Sender     string   `json:"sender"`
	Subject    string   `json:"subject"`
	BodyText   string   `json:"body_text"`
	BodyHTML   string   `json:"body_html"`
	Raw        string   `json:"raw"`
}

type batchResponse struct {
	Results []struct {
		Status string `json:"status"`
	} `json:"results"`
}

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:18080", "API base URL")
	recipient := flag.String("recipient", "mailbox@example.test", "existing mailbox address")
	requests := flag.Int("requests", 10000, "number of HTTP requests")
	concurrency := flag.Int("concurrency", 128, "concurrent workers")
	rawBytes := flag.Int("raw-bytes", 1024, "raw message bytes per request")
	batchSize := flag.Int("batch-size", 1, "recipients per request")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 || *rawBytes < 1 || *batchSize < 1 {
		fmt.Fprintln(os.Stderr, "all numeric flags must be positive")
		os.Exit(2)
	}

	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency,
		MaxConnsPerHost:     *concurrency,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	defer client.CloseIdleConnections()

	jobs := make(chan int, *concurrency)
	latencies := make(chan time.Duration, *requests)
	var successes, failures atomic.Int64
	started := time.Now()
	var workers sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for seq := range jobs {
				begin := time.Now()
				if err := deliver(client, *baseURL, *recipient, *rawBytes, *batchSize, seq); err != nil {
					failures.Add(1)
				} else {
					successes.Add(int64(*batchSize))
				}
				latencies <- time.Since(begin)
			}
		}()
	}
	for i := 0; i < *requests; i++ {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(latencies)
	elapsed := time.Since(started)

	values := make([]time.Duration, 0, *requests)
	for latency := range latencies {
		values = append(values, latency)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) time.Duration {
		if len(values) == 0 {
			return 0
		}
		idx := int(float64(len(values)-1) * p)
		return values[idx]
	}

	fmt.Printf("requests=%d batch_size=%d delivered=%d failures=%d elapsed=%s deliveries_per_second=%.0f p50=%s p95=%s p99=%s\n",
		*requests, *batchSize, successes.Load(), failures.Load(), elapsed.Round(time.Millisecond),
		float64(successes.Load())/elapsed.Seconds(),
		percentile(0.50).Round(time.Microsecond), percentile(0.95).Round(time.Microsecond), percentile(0.99).Round(time.Microsecond),
	)
	if failures.Load() != 0 {
		os.Exit(1)
	}
}

func deliver(client *http.Client, baseURL, recipient string, rawBytes, batchSize, seq int) error {
	raw := fmt.Sprintf("Message-ID: <%d@bench>\r\n\r\n", seq)
	if len(raw) < rawBytes {
		raw += string(bytes.Repeat([]byte("x"), rawBytes-len(raw)))
	}

	var endpoint string
	var payload any
	if batchSize == 1 {
		endpoint = "/internal/deliver"
		payload = singlePayload{Recipient: recipient, Sender: "bench@example.test", Subject: "benchmark", BodyText: "benchmark", Raw: raw}
	} else {
		endpoint = "/internal/deliver-batch"
		recipients := make([]string, batchSize)
		for i := range recipients {
			recipients[i] = recipient
		}
		payload = batchPayload{Recipients: recipients, Sender: "bench@example.test", Subject: "benchmark", BodyText: "benchmark", Raw: raw}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", res.StatusCode, responseBody)
	}
	if batchSize > 1 {
		var parsed batchResponse
		if err := json.Unmarshal(responseBody, &parsed); err != nil {
			return err
		}
		if len(parsed.Results) != batchSize {
			return fmt.Errorf("got %d results, want %d", len(parsed.Results), batchSize)
		}
		for _, result := range parsed.Results {
			if result.Status != "delivered" {
				return fmt.Errorf("unexpected status %q", result.Status)
			}
		}
		return nil
	}
	var parsed struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return err
	}
	if parsed.Status != "delivered" {
		return fmt.Errorf("unexpected status %q", parsed.Status)
	}
	return nil
}

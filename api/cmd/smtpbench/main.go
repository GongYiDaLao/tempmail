package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:2525", "SMTP server address")
	recipient := flag.String("recipient", "mailbox@example.test", "recipient address")
	messages := flag.Int("messages", 10000, "messages to send")
	concurrency := flag.Int("concurrency", 100, "concurrent SMTP workers")
	perConnection := flag.Int("messages-per-connection", 10, "messages sent before reconnecting")
	rawBytes := flag.Int("raw-bytes", 1024, "approximate RFC 5322 message bytes")
	databaseDSN := flag.String("database-dsn", "", "optional PostgreSQL/PgBouncer DSN for committed-row verification")
	verifyTimeout := flag.Duration("verify-timeout", 30*time.Second, "maximum time to wait for committed rows")
	verifyPollInterval := flag.Duration("verify-poll-interval", 250*time.Millisecond, "interval between committed-row checks")
	postqueuePath := flag.String("postqueue-path", "", "optional postqueue binary path for exact queue-drain timing")
	flag.Parse()
	if *databaseDSN == "" {
		*databaseDSN = os.Getenv("BENCH_DATABASE_DSN")
	}
	if *messages < 1 || *concurrency < 1 || *perConnection < 1 || *rawBytes < 128 || *verifyTimeout <= 0 || *verifyPollInterval <= 0 {
		fmt.Fprintln(os.Stderr, "invalid numeric flag")
		os.Exit(2)
	}
	if *postqueuePath != "" && *databaseDSN == "" {
		fmt.Fprintln(os.Stderr, "-postqueue-path requires -database-dsn")
		os.Exit(2)
	}

	var verifyConn *pgx.Conn
	var baseline int64
	runID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	if *databaseDSN != "" {
		cfg, err := pgx.ParseConfig(*databaseDSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse database DSN: %v\n", err)
			os.Exit(2)
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		setupCtx, cancelSetup := context.WithTimeout(context.Background(), *verifyTimeout)
		verifyConn, err = pgx.ConnectConfig(setupCtx, cfg)
		if err != nil {
			cancelSetup()
			fmt.Fprintf(os.Stderr, "connect verification database: %v\n", err)
			os.Exit(2)
		}
		defer func() {
			closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelClose()
			_ = verifyConn.Close(closeCtx)
		}()
		if err := verifyConn.QueryRow(setupCtx, `
			SELECT COUNT(*)
			FROM emails
			WHERE mailbox_id = (SELECT id FROM mailboxes WHERE full_address = lower($1))`,
			*recipient,
		).Scan(&baseline); err != nil {
			cancelSetup()
			fmt.Fprintf(os.Stderr, "read verification baseline: %v\n", err)
			os.Exit(2)
		}
		cancelSetup()
	}

	jobs := make(chan int, *concurrency)
	latencies := make(chan time.Duration, *messages)
	var accepted, failed atomic.Int64
	started := time.Now()
	var workers sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var client *smtp.Client
			var conn net.Conn
			used := 0
			closeClient := func() {
				if client != nil {
					_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
					_ = client.Quit()
					client = nil
					conn = nil
				}
			}
			defer closeClient()

			for seq := range jobs {
				if client == nil || used >= *perConnection {
					closeClient()
					var err error
					client, conn, err = dialSMTP(*addr)
					if err != nil {
						failed.Add(1)
						continue
					}
					used = 0
				}

				begin := time.Now()
				_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
				err := send(client, *recipient, *rawBytes, runID, seq)
				latencies <- time.Since(begin)
				if err != nil {
					failed.Add(1)
					_ = client.Close()
					client = nil
					conn = nil
					continue
				}
				used++
				accepted.Add(1)
			}
		}()
	}
	for i := 0; i < *messages; i++ {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(latencies)
	acceptElapsed := time.Since(started)
	commitElapsed := acceptElapsed
	queueEmptyElapsed := acceptElapsed
	committed := int64(0)
	if verifyConn != nil {
		verifyCtx, cancelVerify := context.WithTimeout(context.Background(), *verifyTimeout)
		for {
			var current int64
			err := verifyConn.QueryRow(verifyCtx, `
				SELECT COUNT(*)
				FROM emails
				WHERE mailbox_id = (SELECT id FROM mailboxes WHERE full_address = lower($1))`,
				*recipient,
			).Scan(&current)
			if err != nil {
				fmt.Fprintf(os.Stderr, "verify committed rows: %v\n", err)
				failed.Add(1)
				break
			}
			committed = current - baseline
			if committed >= accepted.Load() {
				break
			}
			if verifyCtx.Err() != nil {
				fmt.Fprintf(os.Stderr, "timed out waiting for committed rows: got %d, want %d\n", committed, accepted.Load())
				failed.Add(1)
				break
			}
			select {
			case <-time.After(*verifyPollInterval):
			case <-verifyCtx.Done():
			}
		}
		cancelVerify()
		commitElapsed = time.Since(started)
		queueEmptyElapsed = commitElapsed
		if committed > accepted.Load() {
			fmt.Fprintf(os.Stderr, "committed rows exceeded accepted messages: got %d, want %d\n", committed, accepted.Load())
			failed.Add(1)
		}

		if failed.Load() == 0 && *postqueuePath != "" {
			queueCtx, cancelQueue := context.WithTimeout(context.Background(), *verifyTimeout)
			for {
				empty, err := postfixQueueEmpty(queueCtx, *postqueuePath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "verify Postfix queue: %v\n", err)
					failed.Add(1)
					break
				}
				if empty {
					queueEmptyElapsed = time.Since(started)
					break
				}
				if queueCtx.Err() != nil {
					fmt.Fprintln(os.Stderr, "timed out waiting for the Postfix queue to drain")
					failed.Add(1)
					break
				}
				select {
				case <-time.After(*verifyPollInterval):
				case <-queueCtx.Done():
				}
			}
			cancelQueue()
		}

		var runRows, uniqueSequences int64
		markerCtx, cancelMarker := context.WithTimeout(context.Background(), *verifyTimeout)
		if err := verifyConn.QueryRow(markerCtx, `
			SELECT
				COUNT(*),
				COUNT(DISTINCT split_part(split_part(raw_message, 'X-Tempmail-Bench-Sequence: ', 2), E'\r\n', 1))
			FROM emails
			WHERE mailbox_id = (SELECT id FROM mailboxes WHERE full_address = lower($1))
			  AND strpos(raw_message, 'X-Tempmail-Bench-Run: ' || $2 || E'\r\n') > 0`,
			*recipient, runID,
		).Scan(&runRows, &uniqueSequences); err != nil {
			fmt.Fprintf(os.Stderr, "verify benchmark run markers: %v\n", err)
			failed.Add(1)
		} else if runRows != accepted.Load() || uniqueSequences != accepted.Load() {
			fmt.Fprintf(os.Stderr, "run marker mismatch: rows=%d unique_sequences=%d accepted=%d\n", runRows, uniqueSequences, accepted.Load())
			failed.Add(1)
		}
		cancelMarker()
	}

	values := make([]time.Duration, 0, *messages)
	for latency := range latencies {
		values = append(values, latency)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) time.Duration {
		if len(values) == 0 {
			return 0
		}
		return values[int(float64(len(values)-1)*p)]
	}
	fmt.Printf("messages=%d accepted=%d failures=%d concurrency=%d messages_per_connection=%d accept_elapsed=%s accepted_per_second=%.0f p50=%s p95=%s p99=%s",
		*messages, accepted.Load(), failed.Load(), *concurrency, *perConnection, acceptElapsed.Round(time.Millisecond),
		float64(accepted.Load())/acceptElapsed.Seconds(),
		percentile(0.50).Round(time.Microsecond), percentile(0.95).Round(time.Microsecond), percentile(0.99).Round(time.Microsecond),
	)
	if verifyConn != nil {
		fmt.Printf(" committed=%d commit_elapsed=%s committed_per_second=%.0f",
			committed, commitElapsed.Round(time.Millisecond), float64(accepted.Load())/commitElapsed.Seconds())
	}
	if *postqueuePath != "" {
		fmt.Printf(" queue_empty_elapsed=%s queue_empty_per_second=%.0f",
			queueEmptyElapsed.Round(time.Millisecond), float64(accepted.Load())/queueEmptyElapsed.Seconds())
	}
	fmt.Println()
	if failed.Load() != 0 {
		os.Exit(1)
	}
}

func dialSMTP(addr string) (*smtp.Client, net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	client, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return client, conn, nil
}

func postfixQueueEmpty(ctx context.Context, postqueuePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, postqueuePath, "-j")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}
	if err := cmd.Start(); err != nil {
		return false, err
	}

	var firstByte [1]byte
	n, readErr := stdout.Read(firstByte[:])
	if n > 0 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return false, nil
	}
	if readErr != nil && readErr != io.EOF {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return false, readErr
	}
	if err := cmd.Wait(); err != nil {
		return false, err
	}
	return true, nil
}

func send(client *smtp.Client, recipient string, rawBytes int, runID string, seq int) error {
	if err := client.Mail("bench@example.test"); err != nil {
		return err
	}
	if err := client.Rcpt(recipient); err != nil {
		return err
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	header := fmt.Sprintf("From: bench@example.test\r\nTo: %s\r\nSubject: benchmark %d\r\nMessage-ID: <%s-%d@bench.example.test>\r\nX-Tempmail-Bench-Run: %s\r\nX-Tempmail-Bench-Sequence: %d\r\nContent-Type: text/plain\r\n\r\n", recipient, seq, runID, seq, runID, seq)
	message := []byte(header)
	if len(message) < rawBytes {
		message = append(message, bytes.Repeat([]byte("x"), rawBytes-len(message))...)
	}
	if _, err := w.Write(message); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// Package main: LMTP 守护进程版 mail-receiver。
//
// 替代原本每封邮件 fork 一次 Go 二进制的 pipe 模式：
//  1. Postfix 把虚拟域名邮件转发到本进程的 unix socket（LMTP）
//  2. 本进程长期常驻，单进程并发处理多条 LMTP 会话
//  3. 解析邮件后调 API /internal/deliver 入库
//  4. 入库失败时返回 LMTP 4xx，Postfix 把邮件保留在队列里自动重试，不会丢失
//
// 协议参考 RFC 2033 (LMTP)。Postfix 支持把这种 socket 当成 transport。
//
// 启动参数（环境变量）：
//
//	API_URL          API 地址，默认 http://api:8080
//	LMTPD_SOCKET     unix socket 路径，默认 /var/run/postfix/mail-receiver.sock
//	LMTPD_BIND       可选 TCP 监听 host:port，调试用
//	LMTPD_HOSTNAME   LMTP banner 中的主机名
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/mail"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxMessageBytes       = 25 << 20
	maxBatchResponseBytes = 4 << 20
	apiTimeout            = 15 * time.Second
)

type deliverPayload struct {
	Recipient string `json:"recipient"`
	Sender    string `json:"sender"`
	Subject   string `json:"subject"`
	BodyText  string `json:"body_text"`
	BodyHTML  string `json:"body_html"`
	Raw       string `json:"raw"`
}

type deliverBatchPayload struct {
	Recipients []string `json:"recipients"`
	Sender     string   `json:"sender"`
	Subject    string   `json:"subject"`
	BodyText   string   `json:"body_text"`
	BodyHTML   string   `json:"body_html"`
	Raw        string   `json:"raw"`
}

type batchResult struct {
	Recipient string `json:"recipient"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
}

type batchResponse struct {
	Results []batchResult `json:"results"`
}

type server struct {
	hostname   string
	apiURL     string
	httpClient *http.Client
	conns      atomic.Int64
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	apiURL := strings.TrimRight(getenv("API_URL", "http://api:8080"), "/")
	hostname := getenv("LMTPD_HOSTNAME", "mail-receiver.local")

	srv := &server{
		hostname:   hostname,
		apiURL:     apiURL,
		httpClient: newAPIHTTPClient(),
	}

	listeners := make([]net.Listener, 0, 2)

	if sockPath := getenv("LMTPD_SOCKET", "/var/run/postfix/mail-receiver.sock"); sockPath != "" {
		_ = os.Remove(sockPath)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			log.Fatalf("listen unix %s: %v", sockPath, err)
		}
		// Postfix 默认以 postfix 用户连接，需要可写权限
		if err := os.Chmod(sockPath, 0o666); err != nil {
			log.Printf("chmod socket: %v", err)
		}
		log.Printf("LMTP listening on unix:%s", sockPath)
		listeners = append(listeners, l)
	}

	if bind := os.Getenv("LMTPD_BIND"); bind != "" {
		l, err := net.Listen("tcp", bind)
		if err != nil {
			log.Fatalf("listen tcp %s: %v", bind, err)
		}
		log.Printf("LMTP listening on tcp:%s", bind)
		listeners = append(listeners, l)
	}

	if len(listeners) == 0 {
		log.Fatal("no listener configured (LMTPD_SOCKET / LMTPD_BIND)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	for _, l := range listeners {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			srv.acceptLoop(ctx, l)
		}(l)
	}

	<-ctx.Done()
	log.Printf("shutting down, draining %d active conns", srv.conns.Load())
	for _, l := range listeners {
		_ = l.Close()
	}
	wg.Wait()
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		log.Printf("invalid %s=%q, using %d", key, raw, fallback)
		return fallback
	}
	return v
}

func newAPIHTTPClient() *http.Client {
	maxIdle := getenvInt("LMTP_HTTP_MAX_IDLE_CONNS", 512)
	maxIdlePerHost := getenvInt("LMTP_HTTP_MAX_IDLE_CONNS_PER_HOST", 256)
	maxConnsPerHost := getenvInt("LMTP_HTTP_MAX_CONNS_PER_HOST", 512)

	return &http.Client{
		Timeout: apiTimeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          maxIdle,
			MaxIdleConnsPerHost:   maxIdlePerHost,
			MaxConnsPerHost:       maxConnsPerHost,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: apiTimeout,
			ForceAttemptHTTP2:     false,
		},
	}
}

func (s *server) acceptLoop(ctx context.Context, l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			log.Printf("accept: %v", err)
			return
		}
		s.conns.Add(1)
		go func() {
			defer s.conns.Add(-1)
			s.handle(ctx, conn)
		}()
	}
}

type session struct {
	srv        *server
	conn       net.Conn
	r          *bufio.Reader
	w          *bufio.Writer
	from       string
	recipients []string
}

func (s *server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))

	sess := &session{
		srv:  s,
		conn: conn,
		r:    bufio.NewReaderSize(conn, 64*1024),
		w:    bufio.NewWriterSize(conn, 64*1024),
	}
	sess.writeLine("220 %s LMTP ready", s.hostname)

	for {
		line, err := sess.readLine()
		if err != nil {
			return
		}
		cmd, args := parseCommand(line)
		switch cmd {
		case "LHLO":
			sess.writeLine("250-%s", s.hostname)
			sess.writeLine("250-PIPELINING")
			sess.writeLine("250-8BITMIME")
			sess.writeLine("250-SIZE %d", maxMessageBytes)
			sess.writeLine("250 ENHANCEDSTATUSCODES")
		case "MAIL":
			sess.from = extractAddr(args)
			sess.recipients = sess.recipients[:0]
			sess.writeLine("250 2.1.0 OK")
		case "RCPT":
			addr := extractAddr(args)
			if addr == "" {
				sess.writeLine("501 5.1.3 bad recipient")
				continue
			}
			sess.recipients = append(sess.recipients, addr)
			sess.writeLine("250 2.1.5 OK")
		case "DATA":
			if len(sess.recipients) == 0 {
				sess.writeLine("503 5.5.1 no recipients")
				continue
			}
			sess.writeLine("354 end with <CR><LF>.<CR><LF>")
			data, err := sess.readData()
			if err != nil {
				return
			}
			sess.deliverAll(ctx, data)
			sess.from = ""
			sess.recipients = sess.recipients[:0]
			_ = sess.conn.SetDeadline(time.Now().Add(2 * time.Minute))
		case "RSET":
			sess.from = ""
			sess.recipients = sess.recipients[:0]
			sess.writeLine("250 2.1.5 OK")
		case "NOOP":
			sess.writeLine("250 2.0.0 OK")
		case "QUIT":
			sess.writeLine("221 2.0.0 bye")
			return
		case "":
			// 忽略空行
		default:
			sess.writeLine("502 5.5.2 unrecognized %q", cmd)
		}
	}
}

func (s *session) readLine() (string, error) {
	line, err := s.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (s *session) writeLine(format string, args ...interface{}) {
	fmt.Fprintf(s.w, format, args...)
	s.w.WriteString("\r\n")
	_ = s.w.Flush()
}

func (s *session) readData() ([]byte, error) {
	var buf bytes.Buffer
	for {
		line, err := s.r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		if bytes.Equal(line, []byte(".\r\n")) || bytes.Equal(line, []byte(".\n")) {
			break
		}
		// dot-stuffing
		if len(line) > 0 && line[0] == '.' {
			line = line[1:]
		}
		if buf.Len()+len(line) > maxMessageBytes {
			// 仍然继续读完，但只丢弃后续，最后再返回错误
			return nil, fmt.Errorf("message exceeds %d bytes", maxMessageBytes)
		}
		buf.Write(line)
	}
	return buf.Bytes(), nil
}

func (s *session) deliverAll(ctx context.Context, raw []byte) {
	msg, parseErr := mail.ReadMessage(bytes.NewReader(raw))
	var sender, subject, bodyText, bodyHTML string
	if parseErr == nil {
		sender = msg.Header.Get("From")
		subject = msg.Header.Get("Subject")
		bodyText, bodyHTML = extractBodies(msg)
	}
	if sender == "" {
		sender = s.from
	}

	rcpts := make([]string, 0, len(s.recipients))
	for _, r := range s.recipients {
		rcpts = append(rcpts, strings.ToLower(strings.TrimSpace(r)))
	}

	if len(rcpts) > 1 {
		results, err := s.srv.deliverBatch(ctx, deliverBatchPayload{
			Recipients: rcpts,
			Sender:     sender,
			Subject:    subject,
			BodyText:   bodyText,
			BodyHTML:   bodyHTML,
			Raw:        string(raw),
		})
		if err != nil {
			log.Printf("deliver-batch: %v", err)
			s.writeTemporaryFailures(len(rcpts))
			return
		}
		if err := validateBatchResults(rcpts, results); err != nil {
			log.Printf("deliver-batch invalid response: %v", err)
			s.writeTemporaryFailures(len(rcpts))
			return
		}
		for _, r := range results {
			s.writeLine("250 2.0.0 <%s> %s", r.Recipient, r.Status)
		}
		return
	}

	for _, rcpt := range rcpts {
		payload := deliverPayload{
			Recipient: rcpt,
			Sender:    sender,
			Subject:   subject,
			BodyText:  bodyText,
			BodyHTML:  bodyHTML,
			Raw:       string(raw),
		}
		if err := s.srv.deliver(ctx, payload); err != nil {
			log.Printf("deliver %s: %v", rcpt, err)
			s.writeLine("451 4.5.0 temporary delivery failure")
			continue
		}
		s.writeLine("250 2.0.0 <%s> accepted", rcpt)
	}
}

func (s *session) writeTemporaryFailures(count int) {
	for i := 0; i < count; i++ {
		s.writeLine("451 4.5.0 temporary delivery failure")
	}
}

func validateBatchResults(recipients []string, results []batchResult) error {
	if len(results) != len(recipients) {
		return fmt.Errorf("got %d results for %d recipients", len(results), len(recipients))
	}
	for i := range recipients {
		if !strings.EqualFold(strings.TrimSpace(results[i].Recipient), recipients[i]) {
			return fmt.Errorf("result %d recipient mismatch", i)
		}
		if results[i].Status != "delivered" && results[i].Status != "discarded" {
			return fmt.Errorf("result %d has invalid status %q", i, results[i].Status)
		}
	}
	return nil
}

func parseCommand(line string) (cmd, args string) {
	idx := strings.IndexAny(line, " \t")
	if idx == -1 {
		return strings.ToUpper(line), ""
	}
	return strings.ToUpper(line[:idx]), strings.TrimSpace(line[idx+1:])
}

func extractAddr(args string) string {
	idx := strings.IndexByte(args, ':')
	if idx == -1 {
		return ""
	}
	rest := strings.TrimSpace(args[idx+1:])
	rest = strings.TrimSpace(strings.SplitN(rest, " ", 2)[0])
	rest = strings.TrimPrefix(rest, "<")
	rest = strings.TrimSuffix(rest, ">")
	return rest
}

func (s *server) deliver(ctx context.Context, payload deliverPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiURL+"/internal/deliver", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		respBody, _ := readAndDrain(res.Body, 1024)
		return fmt.Errorf("api HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(respBody)))
	}
	_, err = readAndDrain(res.Body, 0)
	return err
}

func (s *server) deliverBatch(ctx context.Context, payload deliverBatchPayload) ([]batchResult, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiURL+"/internal/deliver-batch", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	respBody, readErr := readAndDrain(res.Body, maxBatchResponseBytes)
	if readErr != nil {
		return nil, fmt.Errorf("read batch response: %w", readErr)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("api HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var parsed batchResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("decode batch response: %w", err)
	}
	return parsed.Results, nil
}

// readAndDrain keeps HTTP/1.1 connections reusable. When limit is positive,
// only that many response bytes are retained while the remainder is discarded.
func readAndDrain(r io.Reader, limit int64) ([]byte, error) {
	var kept bytes.Buffer
	if limit > 0 {
		if _, err := io.Copy(&kept, io.LimitReader(r, limit)); err != nil {
			return nil, err
		}
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return nil, err
	}
	return kept.Bytes(), nil
}

func extractBodies(msg *mail.Message) (string, string) {
	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err == nil && strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		return extractMultipart(msg.Body, params["boundary"])
	}
	body, _ := io.ReadAll(io.LimitReader(msg.Body, maxMessageBytes))
	if strings.EqualFold(mediaType, "text/html") {
		return "", string(body)
	}
	return string(body), ""
}

func extractMultipart(r io.Reader, boundary string) (string, string) {
	if boundary == "" {
		body, _ := io.ReadAll(io.LimitReader(r, maxMessageBytes))
		return string(body), ""
	}
	var textBody, htmlBody string
	mr := multipart.NewReader(r, boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		mediaType, params, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
		mediaType = strings.ToLower(mediaType)
		if strings.HasPrefix(mediaType, "multipart/") {
			nt, nh := extractMultipart(part, params["boundary"])
			if textBody == "" {
				textBody = nt
			}
			if htmlBody == "" {
				htmlBody = nh
			}
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(part, maxMessageBytes))
		switch mediaType {
		case "text/plain":
			if textBody == "" {
				textBody = string(body)
			}
		case "text/html":
			if htmlBody == "" {
				htmlBody = string(body)
			}
		}
	}
	return textBody, htmlBody
}

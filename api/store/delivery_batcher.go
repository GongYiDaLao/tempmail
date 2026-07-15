package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	deliveryBatchDefaultMax     = 64
	deliveryBatchMaxLimit       = 256
	deliveryBatchDefaultWait    = time.Millisecond
	deliveryBatchMinWait        = 100 * time.Microsecond
	deliveryBatchMaxWait        = 100 * time.Millisecond
	deliveryBatchMaxBytes       = 4 << 20
	deliveryBatchMaxQueuedBytes = 64 << 20
	deliveryBatchTimeout        = 5 * time.Second
)

var (
	ErrDeliveryBatcherClosed  = errors.New("delivery batcher is closed")
	ErrDeliveryBatchQueueFull = errors.New("delivery batch queue byte limit exceeded")
)

type deliveryBatchJob struct {
	ctx      context.Context
	delivery MessageDelivery
	response chan deliveryBatchResponse
	size     int64
}

type deliveryBatchResponse struct {
	result DeliveredEmail
	err    error
}

// DeliveryBatcher micro-batches concurrent calls to the existing single-mail
// delivery endpoint. Every caller waits for the shared SQL statement to commit
// before receiving success, so Postfix queue durability semantics are retained.
type DeliveryBatcher struct {
	ctx      context.Context
	cancel   context.CancelFunc
	store    *Store
	jobs     chan deliveryBatchJob
	maxBatch int
	maxWait  time.Duration
	done     chan struct{}

	admissionMu sync.Mutex
	closed      bool
	submitters  sync.WaitGroup
	closeOnce   sync.Once
	closeErr    error
	queuedBytes atomic.Int64

	batchCount atomic.Int64
	itemCount  atomic.Int64
	dbNanos    atomic.Int64
	fullFlush  atomic.Int64
	bytesFlush atomic.Int64
	timerFlush atomic.Int64
}

func NewDeliveryBatcher(ctx context.Context, store *Store, maxBatch int, maxWait time.Duration) *DeliveryBatcher {
	if maxBatch < 1 || maxBatch > deliveryBatchMaxLimit {
		maxBatch = deliveryBatchDefaultMax
	}
	if maxWait < deliveryBatchMinWait || maxWait > deliveryBatchMaxWait {
		maxWait = deliveryBatchDefaultWait
	}
	batcherCtx, cancel := context.WithCancel(ctx)
	b := &DeliveryBatcher{
		ctx:      batcherCtx,
		cancel:   cancel,
		store:    store,
		jobs:     make(chan deliveryBatchJob, maxBatch*4),
		maxBatch: maxBatch,
		maxWait:  maxWait,
		done:     make(chan struct{}),
	}
	go b.run()
	go b.logStats()
	return b
}

func (b *DeliveryBatcher) Deliver(ctx context.Context, delivery MessageDelivery) (DeliveredEmail, error) {
	b.admissionMu.Lock()
	if b.closed {
		b.admissionMu.Unlock()
		return DeliveredEmail{}, ErrDeliveryBatcherClosed
	}
	b.submitters.Add(1)
	b.admissionMu.Unlock()
	defer b.submitters.Done()

	if err := validateMessageDelivery(delivery); err != nil {
		// Preserve the existing endpoint contract for malformed mail sent to an
		// unknown recipient. The single statement discards unknown recipients
		// before inserting constrained fields, while a known poison message still
		// fails alone and therefore cannot roll back otherwise-valid batch peers.
		singleCtx, cancel := context.WithTimeout(ctx, deliveryBatchTimeout)
		stopBatcherCancel := context.AfterFunc(b.ctx, cancel)
		result, singleErr := b.store.DeliverMessage(singleCtx, delivery)
		stopBatcherCancel()
		cancel()
		if singleErr != nil {
			return DeliveredEmail{}, singleErr
		}
		return result, nil
	}
	if delivery.ReceivedAt.IsZero() {
		delivery.ReceivedAt = time.Now()
	}

	size := int64(messageDeliveryBytes(delivery))
	if !b.reserveBytes(size) {
		return DeliveredEmail{}, ErrDeliveryBatchQueueFull
	}
	response := make(chan deliveryBatchResponse, 1)
	job := deliveryBatchJob{ctx: ctx, delivery: delivery, response: response, size: size}
	select {
	case b.jobs <- job:
	case <-ctx.Done():
		b.queuedBytes.Add(-size)
		return DeliveredEmail{}, ctx.Err()
	case <-b.ctx.Done():
		b.queuedBytes.Add(-size)
		return DeliveredEmail{}, b.ctx.Err()
	}

	for {
		select {
		case result := <-response:
			return result.result, result.err
		default:
		}
		select {
		case result := <-response:
			return result.result, result.err
		case <-ctx.Done():
			select {
			case result := <-response:
				return result.result, result.err
			default:
				return DeliveredEmail{}, ctx.Err()
			}
		case <-b.ctx.Done():
			select {
			case result := <-response:
				return result.result, result.err
			default:
				return DeliveredEmail{}, b.ctx.Err()
			}
		}
	}
}

func (b *DeliveryBatcher) CloseAndDrain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.closeOnce.Do(func() {
		b.admissionMu.Lock()
		b.closed = true
		b.admissionMu.Unlock()

		submittersDone := make(chan struct{})
		go func() {
			b.submitters.Wait()
			close(submittersDone)
		}()
		select {
		case <-submittersDone:
		case <-ctx.Done():
			b.closeErr = ctx.Err()
			b.cancel()
			<-submittersDone
		}
		close(b.jobs)
		select {
		case <-b.done:
		case <-ctx.Done():
			b.closeErr = ctx.Err()
			b.cancel()
			<-b.done
		}
		b.cancel()
	})
	return b.closeErr
}

func validateMessageDelivery(delivery MessageDelivery) error {
	if utf8.RuneCountInString(delivery.Sender) > 320 {
		return fmt.Errorf("sender exceeds 320 characters")
	}
	if utf8.RuneCountInString(delivery.Subject) > 998 {
		return fmt.Errorf("subject exceeds 998 characters")
	}
	if len(delivery.Raw) > math.MaxInt32 {
		return fmt.Errorf("raw message is too large")
	}
	for name, value := range map[string]string{
		"recipient": delivery.Recipient,
		"sender":    delivery.Sender,
		"subject":   delivery.Subject,
		"body_text": delivery.BodyText,
		"body_html": delivery.BodyHTML,
		"raw":       delivery.Raw,
	} {
		if strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%s contains a NUL byte", name)
		}
	}
	return nil
}

func messageDeliveryBytes(delivery MessageDelivery) int {
	return len(delivery.Recipient) + len(delivery.Sender) + len(delivery.Subject) +
		len(delivery.BodyText) + len(delivery.BodyHTML) + len(delivery.Raw)
}

func (b *DeliveryBatcher) reserveBytes(size int64) bool {
	if size > deliveryBatchMaxQueuedBytes {
		return false
	}
	for {
		current := b.queuedBytes.Load()
		if current+size > deliveryBatchMaxQueuedBytes {
			return false
		}
		if b.queuedBytes.CompareAndSwap(current, current+size) {
			return true
		}
	}
}

func (b *DeliveryBatcher) run() {
	defer close(b.done)
	var pending *deliveryBatchJob
	for {
		var first deliveryBatchJob
		if pending != nil {
			select {
			case <-b.ctx.Done():
				pending.response <- deliveryBatchResponse{err: b.ctx.Err()}
				b.queuedBytes.Add(-pending.size)
				b.failQueued(b.ctx.Err())
				return
			default:
			}
			first = *pending
			pending = nil
		} else {
			select {
			case job, ok := <-b.jobs:
				if !ok {
					return
				}
				first = job
			case <-b.ctx.Done():
				b.failQueued(b.ctx.Err())
				return
			}
		}

		batch := make([]deliveryBatchJob, 0, b.maxBatch)
		batch = append(batch, first)
		totalBytes := messageDeliveryBytes(first.delivery)
		timer := time.NewTimer(b.maxWait)
		closing := false
		hitBytes := false

	collect:
		for len(batch) < b.maxBatch && totalBytes < deliveryBatchMaxBytes {
			select {
			case job, ok := <-b.jobs:
				if !ok {
					closing = true
					break collect
				}
				jobBytes := messageDeliveryBytes(job.delivery)
				if totalBytes+jobBytes > deliveryBatchMaxBytes {
					pending = &job
					hitBytes = true
					break collect
				}
				batch = append(batch, job)
				totalBytes += jobBytes
			case <-timer.C:
				break collect
			case <-b.ctx.Done():
				stopAndDrainTimer(timer)
				b.failBatch(batch, b.ctx.Err())
				b.releaseBatchBytes(batch)
				b.failQueued(b.ctx.Err())
				return
			}
		}
		stopAndDrainTimer(timer)

		switch {
		case len(batch) >= b.maxBatch:
			b.fullFlush.Add(1)
		case hitBytes || totalBytes >= deliveryBatchMaxBytes:
			b.bytesFlush.Add(1)
		default:
			b.timerFlush.Add(1)
		}
		b.execute(batch)
		if closing {
			return
		}
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (b *DeliveryBatcher) execute(batch []deliveryBatchJob) {
	defer b.releaseBatchBytes(batch)
	active := make([]deliveryBatchJob, 0, len(batch))
	deliveries := make([]MessageDelivery, 0, len(batch))
	for _, job := range batch {
		if err := job.ctx.Err(); err != nil {
			job.response <- deliveryBatchResponse{err: err}
			continue
		}
		active = append(active, job)
		deliveries = append(deliveries, job.delivery)
	}
	if len(active) == 0 {
		return
	}

	started := time.Now()
	batchCtx, cancel := context.WithTimeout(b.ctx, deliveryBatchTimeout)
	results, err := b.store.DeliverMessageBatch(batchCtx, deliveries)
	cancel()
	b.batchCount.Add(1)
	b.itemCount.Add(int64(len(active)))
	b.dbNanos.Add(time.Since(started).Nanoseconds())

	if err != nil {
		b.failBatch(active, err)
		return
	}
	for i, job := range active {
		job.response <- deliveryBatchResponse{result: results[i]}
	}
}

func (b *DeliveryBatcher) releaseBatchBytes(batch []deliveryBatchJob) {
	var size int64
	for _, job := range batch {
		size += job.size
	}
	b.queuedBytes.Add(-size)
}

func (b *DeliveryBatcher) failBatch(batch []deliveryBatchJob, err error) {
	for _, job := range batch {
		job.response <- deliveryBatchResponse{err: err}
	}
}

func (b *DeliveryBatcher) failQueued(err error) {
	for {
		select {
		case job, ok := <-b.jobs:
			if !ok {
				return
			}
			job.response <- deliveryBatchResponse{err: err}
			b.queuedBytes.Add(-job.size)
		default:
			return
		}
	}
}

func (b *DeliveryBatcher) logStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.logSnapshot()
		case <-b.done:
			b.logSnapshot()
			return
		}
	}
}

func (b *DeliveryBatcher) logSnapshot() {
	batches := b.batchCount.Load()
	if batches == 0 {
		return
	}
	items := b.itemCount.Load()
	log.Printf("[delivery-batcher] batches=%d items=%d avg_size=%.2f avg_db=%s full=%d bytes=%d timer=%d",
		batches,
		items,
		float64(items)/float64(batches),
		time.Duration(b.dbNanos.Load()/batches).Round(time.Microsecond),
		b.fullFlush.Load(),
		b.bytesFlush.Load(),
		b.timerFlush.Load(),
	)
}

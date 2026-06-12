package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
	"github.com/rstmyldrm7/go-notify/internal/observ"
	"github.com/rstmyldrm7/go-notify/internal/provider"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

// job is a fetched-but-not-yet-committed message moving through one pool. It
// keeps a handle to the consumer it came from so the sender can commit the
// exact partition/offset once the work is done.
type job struct {
	raw      kafka.Message
	priority domain.Priority
	msg      queue.NotificationMessage
	consumer *queue.Consumer
	corrID   string
}

// Deps are the shared collaborators every pool needs.
type Deps struct {
	Brokers     []string
	GroupPrefix string
	Provider    *provider.Client
	Repo        *storage.Repository
	DLQ         *queue.DLQProducer
	Log         *slog.Logger

	Senders     int
	BufferSize  int
	RatePerSec  int
	RateBurst   int
	MaxAttempts int
	Backoff     time.Duration
}

// Pool is the completely isolated delivery pipeline for a single channel. It
// owns three Kafka consumers (high/normal/low), three in-memory priority
// channels, a bank of sender goroutines and the channel's own rate limiter.
// Channels never share goroutines, limiters or back-pressure with each other.
type Pool struct {
	channel   domain.Channel
	consumers map[domain.Priority]*queue.Consumer

	highCh   chan job
	normalCh chan job
	lowCh    chan job

	limiter  *rate.Limiter
	provider *provider.Client
	repo     *storage.Repository
	dlq      *queue.DLQProducer
	log      *slog.Logger

	senders     int
	maxAttempts int
	backoff     time.Duration
}

// NewPool wires a pool for channel: one consumer per priority topic joined to
// the channel's consumer group, and a limiter capping deliveries to the
// configured rate.
func NewPool(channel domain.Channel, d Deps) *Pool {
	group := d.GroupPrefix + "-" + string(channel)
	consumers := make(map[domain.Priority]*queue.Consumer, len(domain.AllPriorities))
	for _, p := range domain.AllPriorities {
		consumers[p] = queue.NewConsumer(d.Brokers, group, queue.TopicFor(channel, p))
	}

	return &Pool{
		channel:     channel,
		consumers:   consumers,
		highCh:      make(chan job, d.BufferSize),
		normalCh:    make(chan job, d.BufferSize),
		lowCh:       make(chan job, d.BufferSize),
		limiter:     rate.NewLimiter(rate.Limit(d.RatePerSec), d.RateBurst),
		provider:    d.Provider,
		repo:        d.Repo,
		dlq:         d.DLQ,
		log:         d.Log.With("channel", string(channel)),
		senders:     d.Senders,
		maxAttempts: d.MaxAttempts,
		backoff:     d.Backoff,
	}
}

// Run starts the fetchers, senders and the depth reporter and blocks until ctx
// is cancelled, then drains and returns once every goroutine has stopped.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// One fetcher per priority topic, each feeding its own in-memory channel.
	for _, pr := range domain.AllPriorities {
		pr := pr
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.fetch(ctx, pr, p.consumers[pr], p.chanFor(pr))
		}()
	}

	// A bank of senders sharing the three priority channels.
	for i := 0; i < p.senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.send(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reportDepth(ctx)
	}()

	p.log.Info("pool started", "senders", p.senders)

	<-ctx.Done()
	// Closing the readers unblocks the fetchers parked in FetchMessage; the
	// senders unblock on ctx.Done() inside next().
	for _, c := range p.consumers {
		_ = c.Close()
	}
	wg.Wait()
	p.log.Info("pool stopped")
}

// fetch reads from one priority topic and forwards each message to the matching
// in-memory channel. A message that cannot even be parsed is a poison pill: it
// is committed immediately so it never blocks the partition.
func (p *Pool) fetch(ctx context.Context, priority domain.Priority, c *queue.Consumer, out chan<- job) {
	for {
		raw, err := c.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("fetch failed", "priority", string(priority), "error", err)
			select {
			case <-time.After(time.Second): // avoid a hot loop on persistent errors
			case <-ctx.Done():
				return
			}
			continue
		}

		var nm queue.NotificationMessage
		if err := json.Unmarshal(raw.Value, &nm); err != nil {
			p.log.Error("unparseable message, committing to skip",
				"priority", string(priority), "error", err)
			_ = c.Commit(ctx, raw)
			continue
		}

		j := job{raw: raw, priority: priority, msg: nm, consumer: c, corrID: queue.CorrelationID(raw)}
		select {
		case out <- j:
		case <-ctx.Done():
			return
		}
	}
}

// send is one sender goroutine: pull the highest-priority available job and
// process it, until the context is cancelled.
func (p *Pool) send(ctx context.Context) {
	for {
		j, ok := p.next(ctx)
		if !ok {
			return
		}
		p.process(ctx, j)
	}
}

// next implements strict priority: it always prefers high over normal over low,
// only blocking (on all three plus ctx) when every channel is currently empty.
func (p *Pool) next(ctx context.Context) (job, bool) {
	// Fast path: take high if anything is waiting there.
	select {
	case j := <-p.highCh:
		return j, true
	default:
	}
	// Then prefer high, fall back to normal, without blocking.
	select {
	case j := <-p.highCh:
		return j, true
	case j := <-p.normalCh:
		return j, true
	default:
	}
	// Everything above empty: block across all priorities and shutdown.
	select {
	case j := <-p.highCh:
		return j, true
	case j := <-p.normalCh:
		return j, true
	case j := <-p.lowCh:
		return j, true
	case <-ctx.Done():
		return job{}, false
	}
}

// process runs a single job end to end: claim the row, rate limit, deliver with
// in-memory retries, then finalize (mark sent, or offload to the DLQ and mark
// dead). The Kafka offset is committed in every terminal case so the pipeline
// never stalls on a poison pill.
func (p *Pool) process(ctx context.Context, j job) {
	cl := string(p.channel)

	// Resume the trace started by the API/scheduler that produced this message,
	// so the consumer span nests under the original request. Downstream DB and
	// provider calls use this context and auto-instrument as child spans.
	ctx = queue.ExtractTrace(ctx, j.raw)
	ctx, span := otel.Tracer(observ.Tracer).Start(ctx, "notification.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("channel", cl),
			attribute.String("priority", string(j.priority)),
			attribute.String("notification.id", j.msg.ID.String()),
			attribute.String("correlation_id", j.corrID),
		),
	)
	defer span.End()

	log := p.log.With(
		"priority", string(j.priority),
		"notification_id", j.msg.ID,
		"correlation_id", j.corrID,
		"trace_id", observ.TraceID(ctx),
	)

	// Claim the row. If it is no longer claimable (cancelled while queued, or
	// already delivered on a redelivery) we skip the send but still advance the
	// offset so we do not see it again.
	claimed, err := p.repo.MarkProcessing(ctx, j.msg.ID)
	if err != nil {
		// Transient DB failure: do NOT commit, let Kafka redeliver later.
		log.Error("mark processing failed, leaving offset uncommitted", "error", err)
		return
	}
	if !claimed {
		log.Info("notification no longer processable, skipping")
		p.commit(ctx, j)
		return
	}

	// Per-channel rate limit (spec: max 100/sec per channel).
	if err := p.limiter.Wait(ctx); err != nil {
		return // ctx cancelled mid-wait; row stays 'processing' and replays
	}

	metrics.Inflight.WithLabelValues(cl).Inc()
	resp, derr := p.deliver(ctx, j, log)
	metrics.Inflight.WithLabelValues(cl).Dec()

	if derr == nil {
		providerID := ""
		if resp != nil {
			providerID = resp.MessageID
		}
		if err := p.repo.MarkSent(ctx, j.msg.ID, providerID); err != nil {
			log.Error("mark sent failed", "error", err)
		}
		metrics.MessagesProcessed.WithLabelValues(cl, string(j.priority), "sent").Inc()
		span.SetAttributes(attribute.String("outcome", "sent"))
		log.Info("delivered", "provider_message_id", providerID)
		p.commit(ctx, j)
		return
	}

	// Retries exhausted or a permanent failure: offload to the DLQ rather than
	// blocking the pipeline, then mark the row dead.
	log.Warn("delivery failed, routing to DLQ", "error", derr)
	span.RecordError(derr)
	span.SetStatus(codes.Error, "delivery failed")
	span.SetAttributes(attribute.String("outcome", "dead"))
	env := queue.DLQEnvelope{
		Original:      j.msg,
		Error:         derr.Error(),
		Attempts:      p.maxAttempts,
		FailedAt:      time.Now(),
		CorrelationID: j.corrID,
	}
	if err := p.dlq.Publish(ctx, p.channel, env); err != nil {
		// We could not even DLQ it; do not commit so it is retried after
		// restart rather than silently lost.
		log.Error("dlq publish failed, leaving offset uncommitted", "error", err)
		return
	}
	if err := p.repo.MarkDead(ctx, j.msg.ID, derr.Error()); err != nil {
		log.Error("mark dead failed", "error", err)
	}
	metrics.MessagesProcessed.WithLabelValues(cl, string(j.priority), "dead").Inc()
	p.commit(ctx, j)
}

// deliver attempts delivery with a fast in-memory retry loop. Permanent (4xx)
// errors short-circuit immediately; retryable ones back off linearly.
func (p *Pool) deliver(ctx context.Context, j job, log *slog.Logger) (*provider.Response, error) {
	req := provider.Request{To: j.msg.Recipient, Channel: j.msg.Channel, Content: j.msg.Content}
	cl := string(p.channel)

	var lastErr error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		start := time.Now()
		resp, err := p.provider.Send(ctx, req)
		metrics.DeliveryDuration.WithLabelValues(cl).Observe(time.Since(start).Seconds())
		if err == nil {
			metrics.ProviderAttempts.WithLabelValues(cl, "success").Inc()
			return resp, nil
		}
		lastErr = err

		var perr *provider.Error
		if errors.As(err, &perr) && !perr.Retryable {
			metrics.ProviderAttempts.WithLabelValues(cl, "permanent_error").Inc()
			return nil, err // no point retrying a 4xx
		}
		metrics.ProviderAttempts.WithLabelValues(cl, "retryable_error").Inc()

		if attempt < p.maxAttempts {
			backoff := p.backoff * time.Duration(attempt) // linear: 500ms, 1s, ...
			log.Debug("retrying delivery", "attempt", attempt, "backoff", backoff.String(), "error", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return nil, lastErr
}

// commit advances the Kafka offset past j. If the pool is shutting down it uses
// a fresh short-lived context so an offset we already finished processing is
// still recorded.
func (p *Pool) commit(ctx context.Context, j job) {
	cctx := ctx
	if cctx.Err() != nil {
		var cancel context.CancelFunc
		cctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	if err := j.consumer.Commit(cctx, j.raw); err != nil {
		p.log.Error("commit failed", "error", err)
	}
}

// reportDepth publishes the live in-memory queue depths once a second.
func (p *Pool) reportDepth(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	cl := string(p.channel)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			metrics.QueueDepth.WithLabelValues(cl, "high").Set(float64(len(p.highCh)))
			metrics.QueueDepth.WithLabelValues(cl, "normal").Set(float64(len(p.normalCh)))
			metrics.QueueDepth.WithLabelValues(cl, "low").Set(float64(len(p.lowCh)))
		}
	}
}

func (p *Pool) chanFor(pr domain.Priority) chan job {
	switch pr {
	case domain.PriorityHigh:
		return p.highCh
	case domain.PriorityLow:
		return p.lowCh
	default:
		return p.normalCh
	}
}

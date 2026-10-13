package management

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	// MaxIdentityBytes bounds worker, backend, queue, and capability labels.
	MaxIdentityBytes = 256
	// MaxQueuesPerWorker bounds one heartbeat's queue cardinality.
	MaxQueuesPerWorker = 256
	// MaxCapabilitiesPerWorker bounds one heartbeat's capability cardinality.
	MaxCapabilitiesPerWorker = 64
	// MaxWorkerConcurrency bounds reported goroutine concurrency.
	MaxWorkerConcurrency = 1_000_000
	// MaxStatusPageSize bounds one worker or queue status response.
	MaxStatusPageSize = 200
)

// WorkerState is the state reported by a live worker.
type WorkerState string

const (
	// WorkerRunning presents active worker intake.
	WorkerRunning WorkerState = "running"
	// WorkerPaused presents suspended intake.
	WorkerPaused WorkerState = "paused"
	// WorkerDraining presents intake withdrawal while work drains.
	WorkerDraining WorkerState = "draining"
	// WorkerStopped presents stopped worker state, not exactly-once delivery.
	WorkerStopped WorkerState = "stopped"
)

// DrainState describes progress of a graceful drain request.
type DrainState string

const (
	// DrainNotRequested means no drain is reported.
	DrainNotRequested DrainState = "not_requested"
	// DrainRequested means drain was requested but progress is not yet reported.
	DrainRequested DrainState = "requested"
	// DrainInProgress means admissions or jobs are still owned.
	DrainInProgress DrainState = "in_progress"
	// DrainCompleted means the provider reports its drain boundary completed.
	DrainCompleted DrainState = "completed"
	// DrainTimedOut means the drain budget expired, not that the requested state was rolled back.
	DrainTimedOut DrainState = "timed_out"
)

// WorkerStatus is the bounded status contract emitted by a worker.
type WorkerStatus struct {
	// ID identifies this worker report, bounded by MaxIdentityBytes.
	ID string
	// Version is the reporter/application version label, not protocol negotiation.
	Version string
	// StartedAt is the required nonzero process start time.
	StartedAt time.Time
	// HeartbeatAt is the report time and must not precede StartedAt.
	HeartbeatAt time.Time
	// Queues contains mutable logical queue labels, at most MaxQueuesPerWorker.
	Queues []string
	// Concurrency reports configured concurrency, from one to MaxWorkerConcurrency.
	Concurrency uint32
	// State is the reported or requested lifecycle state, not transport connectivity proof.
	State WorkerState
	// CurrentJobs must not exceed Concurrency in a valid report.
	CurrentJobs uint32
	// DrainStatus describes requested drain progress.
	DrainStatus DrainState
	// Backend identifies the reporting transport, not its address or credentials.
	Backend string
	// Protocol is the management protocol version; zero is unknown or invalid.
	Protocol ProtocolVersion
	// Capabilities contains mutable bounded capability claims, not authorization grants.
	Capabilities []Capability
}

// StatusMetadata configures stable identity for a native worker reporter.
type StatusMetadata struct {
	// ID is the provider-owned identity, not an authenticated credential.
	ID string
	// Version is the reporter/application version label, not protocol negotiation.
	Version string
	// Concurrency is configured reporting concurrency, from one to MaxWorkerConcurrency.
	Concurrency uint32
	// Protocol is the management protocol version; zero is unknown or invalid.
	Protocol ProtocolVersion
}

// Validate rejects incomplete or unbounded native reporter metadata.
func (m StatusMetadata) Validate() error {
	if invalidIdentity(m.ID) {
		return invalid("id", "is required and must be bounded")
	}
	if invalidIdentity(m.Version) {
		return invalid("version", "is required and must be bounded")
	}
	if m.Concurrency == 0 || m.Concurrency > MaxWorkerConcurrency {
		return invalid("concurrency", "must be within the worker limit")
	}
	if m.Protocol == (ProtocolVersion{}) {
		return invalid("protocol", "is required")
	}

	return nil
}

// Measurement distinguishes an honestly measured zero from an unsupported
// backend metric.
type Measurement[T any] struct {
	// Value is meaningful only when Supported is true.
	Value T
	// Supported distinguishes an actual measured zero from an unavailable value.
	Supported bool
}

// QueueMetrics reports current gauges and monotonic lifecycle counters where
// the backend can measure them honestly.
type QueueMetrics struct {
	// Depth reports outstanding work under the provider's scope; stream backends
	// combine pending and known lag.
	Depth Measurement[int64]
	// Lag counts undelivered group entries when supported; unknown lag uses
	// Supported=false rather than a supported negative value.
	Lag Measurement[int64]
	// Pending counts entries delivered to the consumer group but not acknowledged.
	Pending Measurement[int64]
	// OldestAge reports age of the backend's oldest supported outstanding entry.
	OldestAge Measurement[time.Duration]
	// Throughput is a finite nonnegative backend-reported rate when supported.
	Throughput Measurement[float64]
	// Runtime reports backend-measured elapsed runtime when supported.
	Runtime Measurement[time.Duration]
	// Succeeded counts backend-observed success under that provider's scope.
	Succeeded Measurement[uint64]
	// Failed counts backend-observed failures under that provider's scope.
	Failed Measurement[uint64]
	// Retried counts backend-observed retry deliveries, not necessarily local handler attempts.
	Retried Measurement[uint64]
	// Reclaimed counts recovered pending entries under the provider's scope.
	Reclaimed Measurement[uint64]
	// DeadLettered counts terminal transfers under the provider's scope.
	DeadLettered Measurement[uint64]
	// SettlementErrors counts failed backend settlement operations under the provider's scope.
	SettlementErrors Measurement[uint64]
}

// QueueStatus is a point-in-time backend-neutral queue observation.
type QueueStatus struct {
	// Backend identifies the reporting transport, not its address or credentials.
	Backend string
	// Queue identifies the logical queue, not a connection URI.
	Queue string
	// ObservedAt is the nonzero report time.
	ObservedAt time.Time
	// Metrics retains support flags instead of inventing unsupported measurements.
	Metrics QueueMetrics
}

// StatusPageRequest defines bounded opaque-cursor status pagination.
type StatusPageRequest struct {
	// Cursor is provider-specific pagination state; empty begins traversal.
	Cursor string
	// Limit is the positive requested item ceiling, checked against the applicable limit.
	Limit uint32
}

// Validate rejects unbounded status requests.
func (r StatusPageRequest) Validate() error {
	if r.Limit == 0 || r.Limit > MaxStatusPageSize {
		return invalid("limit", "must be within the page limit")
	}
	if len(r.Cursor) > MaxCursorBytes {
		return invalid("cursor", "exceeds the cursor limit")
	}

	return nil
}

// WorkerStatusPage is one bounded page of worker observations.
type WorkerStatusPage struct {
	// Items contains this page's records; slice storage is mutable and not copied by validation.
	Items []WorkerStatus
	// NextCursor is provider-specific continuation state; empty finishes traversal.
	NextCursor string
}

// Validate rejects malformed or oversized worker adapter output.
func (p WorkerStatusPage) Validate() error {
	if len(p.Items) > MaxStatusPageSize {
		return invalid("items", "exceeds the page limit")
	}
	for index, item := range p.Items {
		if err := item.Validate(); err != nil {
			var validationError *ValidationError
			_ = errors.As(err, &validationError)

			return invalid(
				fmt.Sprintf("items[%d].%s", index, validationError.Field),
				validationError.Problem,
			)
		}
	}
	if len(p.NextCursor) > MaxCursorBytes {
		return invalid("next_cursor", "exceeds the cursor limit")
	}

	return nil
}

// QueueStatusPage is one bounded page of queue observations.
type QueueStatusPage struct {
	// Items contains this page's records; slice storage is mutable and not copied by validation.
	Items []QueueStatus
	// NextCursor is provider-specific continuation state; empty finishes traversal.
	NextCursor string
}

// Validate rejects malformed or oversized queue adapter output.
func (p QueueStatusPage) Validate() error {
	if len(p.Items) > MaxStatusPageSize {
		return invalid("items", "exceeds the page limit")
	}
	for index, item := range p.Items {
		if err := item.Validate(); err != nil {
			var validationError *ValidationError
			_ = errors.As(err, &validationError)

			return invalid(
				fmt.Sprintf("items[%d].%s", index, validationError.Field),
				validationError.Problem,
			)
		}
	}
	if len(p.NextCursor) > MaxCursorBytes {
		return invalid("next_cursor", "exceeds the cursor limit")
	}

	return nil
}

// StatusReader is implemented by queue adapters that expose bounded
// backend-neutral worker and queue observations.
type StatusReader interface {
	// ListWorkers returns a bounded page under provider-owned cursor/snapshot semantics.
	ListWorkers(context.Context, StatusPageRequest) (WorkerStatusPage, error)
	// ListQueues returns a bounded page under provider-owned cursor/snapshot semantics.
	ListQueues(context.Context, StatusPageRequest) (QueueStatusPage, error)
}

// Validate rejects malformed or unbounded worker reports.
func (s WorkerStatus) Validate() error {
	if invalidIdentity(s.ID) {
		return invalid("id", "is required and must be bounded")
	}
	if invalidIdentity(s.Version) {
		return invalid("version", "is required and must be bounded")
	}
	if s.StartedAt.IsZero() {
		return invalid("started_at", "is required")
	}
	if s.HeartbeatAt.IsZero() {
		return invalid("heartbeat_at", "is required")
	}
	if s.HeartbeatAt.Before(s.StartedAt) {
		return invalid("heartbeat_at", "cannot precede started_at")
	}
	if len(s.Queues) > MaxQueuesPerWorker {
		return invalid("queues", "exceeds the worker queue limit")
	}
	for index, queue := range s.Queues {
		if invalidIdentity(queue) {
			return invalid(fmt.Sprintf("queues[%d]", index), "is invalid")
		}
	}
	if s.Concurrency == 0 || s.Concurrency > MaxWorkerConcurrency {
		return invalid("concurrency", "must be within the worker limit")
	}
	if s.CurrentJobs > s.Concurrency {
		return invalid("current_jobs", "cannot exceed concurrency")
	}
	if !s.State.valid() {
		return invalid("state", "is unsupported")
	}
	if !s.DrainStatus.valid() {
		return invalid("drain_status", "is unsupported")
	}
	if invalidIdentity(s.Backend) {
		return invalid("backend", "is required and must be bounded")
	}
	if s.Protocol == (ProtocolVersion{}) {
		return invalid("protocol", "is required")
	}
	if len(s.Capabilities) > MaxCapabilitiesPerWorker {
		return invalid("capabilities", "exceeds the worker capability limit")
	}
	for index, capability := range s.Capabilities {
		if invalidIdentity(string(capability)) {
			return invalid(fmt.Sprintf("capabilities[%d]", index), "is invalid")
		}
	}

	return nil
}

// Validate rejects malformed queue observations.
func (s QueueStatus) Validate() error {
	if invalidIdentity(s.Backend) {
		return invalid("backend", "is required and must be bounded")
	}
	if invalidIdentity(s.Queue) {
		return invalid("queue", "is required and must be bounded")
	}
	if s.ObservedAt.IsZero() {
		return invalid("observed_at", "is required")
	}
	if s.Metrics.Depth.Supported && s.Metrics.Depth.Value < 0 {
		return invalid("metrics.depth", "cannot be negative")
	}
	if s.Metrics.Lag.Supported && s.Metrics.Lag.Value < 0 {
		return invalid("metrics.lag", "cannot be negative")
	}
	if s.Metrics.Pending.Supported && s.Metrics.Pending.Value < 0 {
		return invalid("metrics.pending", "cannot be negative")
	}
	if s.Metrics.OldestAge.Supported && s.Metrics.OldestAge.Value < 0 {
		return invalid("metrics.oldest_age", "cannot be negative")
	}
	if s.Metrics.Throughput.Supported &&
		(s.Metrics.Throughput.Value < 0 || math.IsNaN(s.Metrics.Throughput.Value) ||
			math.IsInf(s.Metrics.Throughput.Value, 0)) {
		return invalid("metrics.throughput", "must be finite and non-negative")
	}
	if s.Metrics.Runtime.Supported && s.Metrics.Runtime.Value < 0 {
		return invalid("metrics.runtime", "cannot be negative")
	}

	return nil
}

func (s WorkerState) valid() bool {
	switch s {
	case WorkerRunning, WorkerPaused, WorkerDraining, WorkerStopped:
		return true
	default:
		return false
	}
}

func (s DrainState) valid() bool {
	switch s {
	case DrainNotRequested, DrainRequested, DrainInProgress, DrainCompleted, DrainTimedOut:
		return true
	default:
		return false
	}
}

func invalidIdentity(value string) bool {
	return strings.TrimSpace(value) == "" || len(value) > MaxIdentityBytes
}

// ValidationError identifies one invalid management-contract field.
type ValidationError struct {
	// Field identifies a validation field; caller construction must keep the label safe.
	Field string
	// Problem describes the rejected shape; caller construction controls its disclosure.
	Problem string
}

// Error implements error.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Problem)
}

func invalid(field, problem string) error {
	return &ValidationError{Field: field, Problem: problem}
}

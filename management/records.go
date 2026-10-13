package management

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// CurrentEnvelopeVersion is the first complete dead-letter record contract.
	CurrentEnvelopeVersion uint16 = 1
	// MaxAdministrativePayloadBytes bounds privileged payload inspection.
	MaxAdministrativePayloadBytes = 1 << 20
	// MaxFailureSummaryBytes bounds deliberately redacted operator diagnostics.
	MaxFailureSummaryBytes = 1_024
	// MaxRecordTags bounds user-supplied dead-letter record dimensions.
	MaxRecordTags = 32
	// MaxPageSize bounds one failure or dead-letter listing response.
	MaxPageSize uint32 = 200
	// MaxCursorBytes bounds opaque pagination state accepted from a client.
	MaxCursorBytes = 1_024
	// MaxSearchBytes bounds backend-neutral administrative search input.
	MaxSearchBytes = 256
)

// RecordKind distinguishes failed jobs from terminal dead letters.
type RecordKind string

const (
	// RecordFailure identifies a failed delivery attempt, not necessarily terminal work.
	RecordFailure RecordKind = "failure"
	// RecordDeadLetter identifies a terminal delivery record.
	RecordDeadLetter RecordKind = "dead_letter"
)

// PayloadVisibility describes the disclosure applied by an authorized
// inspection. Its zero value is hidden so payloads are never exposed by
// default.
type PayloadVisibility string

const (
	// PayloadHidden omits data and is the fail-closed zero visibility.
	PayloadHidden PayloadVisibility = ""
	// PayloadRedacted marks nonrevealed disclosure; Data still must be empty.
	PayloadRedacted PayloadVisibility = "redacted"
	// PayloadRevealed permits bounded Data after caller-owned authorization.
	PayloadRevealed PayloadVisibility = "revealed"
)

// Payload is a bounded administrative representation. Data must remain empty
// unless privileged access explicitly requested revealed content.
type Payload struct {
	// Visibility selects disclosure; hidden is the zero value, not an authorization check.
	Visibility PayloadVisibility
	// ContentType is an optional application media-type label, not verified payload content.
	ContentType string
	// Size reports original byte size and may exceed revealed Data length.
	Size int64
	// Data is mutable; value copies share its backing array. Validation permits
	// bytes only when revealed.
	Data []byte
}

// JobRecord is the backend-neutral failure or dead-letter representation.
type JobRecord struct {
	// Kind selects the supported resource or record category.
	Kind RecordKind
	// ID identifies this backend failure/dead-letter record, separately from OriginalID.
	ID string
	// Backend identifies the reporting transport, not its address or credentials.
	Backend string
	// Queue identifies the logical queue, not a connection URI.
	Queue string
	// OccurredAt is the backend/event time of this observation, not a universal ordering clock.
	OccurredAt time.Time
	// Attempts counts backend delivery attempts, independently of local handler retries.
	Attempts uint32
	// FailureCode is an operational failure label, not arbitrary error text.
	FailureCode string
	// Payload holds disclosure-controlled mutable application bytes.
	Payload Payload

	// EnvelopeVersion is zero for basic legacy records or CurrentEnvelopeVersion for extended metadata.
	EnvelopeVersion uint16
	// PayloadSchemaVersion is optional application schema identity; this package does not validate that schema.
	PayloadSchemaVersion string
	// OriginalID is optional application identity, distinct from backend source identity.
	OriginalID string
	// Topic is optional source topic identity.
	Topic string
	// Stream is the logical stream key, not a transport endpoint.
	Stream string
	// RoutingKey is optional source routing identity.
	RoutingKey string
	// ConsumerGroup is optional source consumer-group identity.
	ConsumerGroup string
	// SourceRecordID identifies the original backend source entry when known.
	SourceRecordID string
	// EnqueuedAt is an optional nonzero enqueue time; nil means unknown.
	EnqueuedAt *time.Time
	// FirstDeliveryAt is optional first-delivery time; nil means unknown.
	FirstDeliveryAt *time.Time
	// LastDeliveryAt is optional last-delivery time, ordered after FirstDeliveryAt when both known.
	LastDeliveryAt *time.Time
	// DeadLetteredAt is required for a versioned terminal record.
	DeadLetteredAt *time.Time
	// RetryPolicy is optional application policy identity, not executable retry configuration.
	RetryPolicy string
	// Classification selects the recorded backend-neutral failure disposition.
	Classification Classification
	// FailureSummary is deliberately safe operator text; Validate bounds but does not redact it.
	FailureSummary string
	// Diagnostics uses independent disclosure bounds; it must not expose bytes implicitly.
	Diagnostics Payload
	// HandlerType is an optional handler label, not serialized code.
	HandlerType string
	// JobType is optional application job identity.
	JobType string
	// Tags contains mutable application dimensions with bounded keys, values and count.
	Tags map[string]string
	// TraceID is optional caller-supplied trace identity, not authenticated evidence.
	TraceID string
	// TenantID is optional caller-supplied tenancy metadata, not access enforcement.
	TenantID string
	// ProducerVersion identifies the producer application when known.
	ProducerVersion string
	// WorkerVersion identifies the handling application when known.
	WorkerVersion string
	// OriginalDeadLetterID identifies the first terminal record in replay lineage.
	OriginalDeadLetterID string
	// PriorDeadLetterID identifies the terminal record immediately preceding this replay.
	PriorDeadLetterID string
	// ReplayGeneration is positive only with complete original/prior lineage; zero means no replay metadata.
	ReplayGeneration uint32
	// RetentionDeadline is optional nonzero expiry, later than DeadLetteredAt when both known.
	RetentionDeadline *time.Time
}

// Validate checks record shape, chronology and disclosure byte bounds. It
// neither authenticates metadata nor redacts FailureSummary, tags or identity
// content, and it does not copy mutable data. Reveal authorization is external.
func (r JobRecord) Validate() error {
	if !r.Kind.valid() {
		return invalid("kind", "is unsupported")
	}
	if invalidIdentity(r.ID) {
		return invalid("id", "is required and must be bounded")
	}
	if invalidIdentity(r.Backend) {
		return invalid("backend", "is required and must be bounded")
	}
	if invalidIdentity(r.Queue) {
		return invalid("queue", "is required and must be bounded")
	}
	if r.OccurredAt.IsZero() {
		return invalid("occurred_at", "is required")
	}
	if r.Attempts == 0 {
		return invalid("attempts", "must be positive")
	}
	if invalidIdentity(r.FailureCode) {
		return invalid("failure_code", "is required and must be bounded")
	}

	if err := r.Payload.validate(); err != nil {
		return err
	}
	if r.EnvelopeVersion == 0 {
		if r.hasV1Metadata() {
			return invalid("envelope_version", "is required for v1 metadata")
		}
		return nil
	}
	if r.EnvelopeVersion != CurrentEnvelopeVersion {
		return invalid("envelope_version", "is unsupported")
	}
	if !r.Classification.valid() {
		return invalid("classification", "is unsupported")
	}
	if r.Kind == RecordDeadLetter && (r.DeadLetteredAt == nil || r.DeadLetteredAt.IsZero()) {
		return invalid("dead_lettered_at", "is required")
	}
	if len(r.FailureSummary) > MaxFailureSummaryBytes {
		return invalid("failure_summary", "exceeds the summary limit")
	}
	if err := r.Diagnostics.validate(); err != nil {
		var validationError *ValidationError
		_ = errors.As(err, &validationError)

		return invalid("diagnostics."+validationError.Field, validationError.Problem)
	}
	if len(r.Tags) > MaxRecordTags {
		return invalid("tags", "exceeds the tag limit")
	}
	for key, value := range r.Tags {
		if invalidIdentity(key) || invalidIdentity(value) {
			return invalid("tags", "keys and values must be bounded")
		}
	}
	for field, value := range map[string]string{
		"payload_schema_version":  r.PayloadSchemaVersion,
		"original_id":             r.OriginalID,
		"topic":                   r.Topic,
		"stream":                  r.Stream,
		"routing_key":             r.RoutingKey,
		"consumer_group":          r.ConsumerGroup,
		"source_record_id":        r.SourceRecordID,
		"retry_policy":            r.RetryPolicy,
		"handler_type":            r.HandlerType,
		"job_type":                r.JobType,
		"trace_id":                r.TraceID,
		"tenant_id":               r.TenantID,
		"producer_version":        r.ProducerVersion,
		"worker_version":          r.WorkerVersion,
		"original_dead_letter_id": r.OriginalDeadLetterID,
		"prior_dead_letter_id":    r.PriorDeadLetterID,
	} {
		if value != "" && invalidIdentity(value) {
			return invalid(field, "must be bounded when known")
		}
	}
	if field := invalidRecordChronology(r); field != "" {
		return invalid(field, "is inconsistent with record chronology")
	}
	if r.ReplayGeneration > 0 && r.OriginalDeadLetterID == "" {
		return invalid("original_dead_letter_id", "is required for replay lineage")
	}
	if r.ReplayGeneration > 0 && r.PriorDeadLetterID == "" {
		return invalid("prior_dead_letter_id", "is required for replay lineage")
	}

	return nil
}

func (r JobRecord) hasV1Metadata() bool {
	return r.PayloadSchemaVersion != "" || r.OriginalID != "" || r.Topic != "" ||
		r.Stream != "" || r.RoutingKey != "" || r.ConsumerGroup != "" ||
		r.SourceRecordID != "" || r.EnqueuedAt != nil || r.FirstDeliveryAt != nil ||
		r.LastDeliveryAt != nil || r.DeadLetteredAt != nil || r.RetryPolicy != "" ||
		r.Classification != "" || r.FailureSummary != "" ||
		r.Diagnostics.Visibility != PayloadHidden || r.Diagnostics.ContentType != "" ||
		r.Diagnostics.Size != 0 || len(r.Diagnostics.Data) != 0 || r.HandlerType != "" ||
		r.JobType != "" || len(r.Tags) != 0 || r.TraceID != "" || r.TenantID != "" ||
		r.ProducerVersion != "" || r.WorkerVersion != "" ||
		r.OriginalDeadLetterID != "" || r.PriorDeadLetterID != "" ||
		r.ReplayGeneration != 0 || r.RetentionDeadline != nil
}

func invalidRecordChronology(record JobRecord) string {
	knownTimes := map[string]*time.Time{
		"enqueued_at":        record.EnqueuedAt,
		"first_delivery_at":  record.FirstDeliveryAt,
		"last_delivery_at":   record.LastDeliveryAt,
		"dead_lettered_at":   record.DeadLetteredAt,
		"retention_deadline": record.RetentionDeadline,
	}
	for field, value := range knownTimes {
		if value != nil && value.IsZero() {
			return field
		}
	}
	if record.EnqueuedAt != nil && record.FirstDeliveryAt != nil &&
		record.FirstDeliveryAt.Before(*record.EnqueuedAt) {
		return "first_delivery_at"
	}
	if record.FirstDeliveryAt != nil && record.LastDeliveryAt != nil &&
		record.LastDeliveryAt.Before(*record.FirstDeliveryAt) {
		return "last_delivery_at"
	}
	if record.LastDeliveryAt != nil && record.DeadLetteredAt != nil &&
		record.DeadLetteredAt.Before(*record.LastDeliveryAt) {
		return "dead_lettered_at"
	}
	if record.DeadLetteredAt != nil && record.RetentionDeadline != nil &&
		!record.RetentionDeadline.After(*record.DeadLetteredAt) {
		return "retention_deadline"
	}

	return ""
}

func (r RecordKind) valid() bool {
	switch r {
	case RecordFailure, RecordDeadLetter:
		return true
	default:
		return false
	}
}

func (p Payload) validate() error {
	if !p.Visibility.valid() {
		return invalid("payload.visibility", "is unsupported")
	}
	if p.Size < 0 {
		return invalid("payload.size", "cannot be negative")
	}
	if p.ContentType != "" && invalidIdentity(p.ContentType) {
		return invalid("payload.content_type", "must be bounded")
	}
	if p.Visibility != PayloadRevealed && len(p.Data) != 0 {
		return invalid("payload.data", "must be hidden")
	}
	if len(p.Data) > MaxAdministrativePayloadBytes {
		return invalid("payload.data", "exceeds the inspection limit")
	}
	if int64(len(p.Data)) > p.Size {
		return invalid("payload.size", "cannot be smaller than revealed data")
	}

	return nil
}

func (v PayloadVisibility) valid() bool {
	switch v {
	case PayloadHidden, PayloadRedacted, PayloadRevealed:
		return true
	default:
		return false
	}
}

// SortField identifies bounded record-list ordering.
type SortField string

const (
	// SortOccurredAt requests observation-time order.
	SortOccurredAt SortField = "occurred_at"
	// SortQueue requests logical queue order where supported.
	SortQueue SortField = "queue"
	// SortAttempts requests delivery-attempt order where supported.
	SortAttempts SortField = "attempts"
)

// SortDirection identifies ascending or descending ordering.
type SortDirection string

const (
	// SortAscending requests forward ordering.
	SortAscending SortDirection = "asc"
	// SortDescending requests reverse ordering.
	SortDescending SortDirection = "desc"
)

// PageRequest defines bounded cursor pagination, sorting, and search.
type PageRequest struct {
	// Cursor is provider-specific pagination state; empty begins traversal.
	Cursor string
	// Limit is the positive requested item ceiling, checked against the applicable limit.
	Limit uint32
	// Search is bounded provider-defined metadata search, not payload search.
	Search string
	// Sort selects requested ordering; a provider may reject unsupported fields.
	Sort SortField
	// Direction selects ascending or descending traversal.
	Direction SortDirection
}

// Validate rejects unbounded or unsupported record-list requests.
func (r PageRequest) Validate() error {
	if r.Limit == 0 || r.Limit > MaxPageSize {
		return invalid("limit", "must be within the page limit")
	}
	if len(r.Cursor) > MaxCursorBytes {
		return invalid("cursor", "exceeds the cursor limit")
	}
	if len(r.Search) > MaxSearchBytes {
		return invalid("search", "exceeds the search limit")
	}
	if !r.Sort.valid() {
		return invalid("sort", "is unsupported")
	}
	if !r.Direction.valid() {
		return invalid("direction", "is unsupported")
	}

	return nil
}

func (s SortField) valid() bool {
	switch s {
	case SortOccurredAt, SortQueue, SortAttempts:
		return true
	default:
		return false
	}
}

func (d SortDirection) valid() bool {
	switch d {
	case SortAscending, SortDescending:
		return true
	default:
		return false
	}
}

// RecordPage is one bounded page of failure or dead-letter metadata.
type RecordPage struct {
	// Items contains this page's records; slice storage is mutable and not copied by validation.
	Items []JobRecord
	// NextCursor is provider-specific continuation state; empty finishes traversal.
	NextCursor string
}

// Validate rejects oversized cursors, pages, and malformed adapter records.
func (p RecordPage) Validate() error {
	if len(p.Items) > int(MaxPageSize) {
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

// InspectRequest asks for one record at an explicit payload visibility.
type InspectRequest struct {
	// Kind selects the supported resource or record category.
	Kind RecordKind
	// ID is the provider-owned identity, not an authenticated credential.
	ID string
	// Visibility selects disclosure; hidden is the zero value, not an authorization check.
	Visibility PayloadVisibility
}

// Validate rejects ambiguous or unbounded inspection requests.
func (r InspectRequest) Validate() error {
	if !r.Kind.valid() {
		return invalid("kind", "is unsupported")
	}
	if invalidIdentity(r.ID) {
		return invalid("id", "is required and must be bounded")
	}
	if !r.Visibility.valid() {
		return invalid("visibility", "is unsupported")
	}

	return nil
}

// RecordReader is implemented by queue adapters that expose safe failure
// and dead-letter inspection without leaking native backend clients.
type RecordReader interface {
	// ListFailures lists bounded metadata with payload and diagnostics hidden.
	ListFailures(context.Context, PageRequest) (RecordPage, error)
	// ListDeadLetters lists bounded terminal metadata with payload and diagnostics hidden.
	ListDeadLetters(context.Context, PageRequest) (RecordPage, error)
	// Inspect returns one record with no more disclosure than requested; authorization is caller-owned.
	Inspect(context.Context, InspectRequest) (JobRecord, error)
}

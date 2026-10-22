// Package job defines mutable execution envelopes encoded with encoding/json.
// Local task functions and settlement callbacks are not serialized. Admission
// borrows payload bytes, while encoding and decoding allocate wire values.
// Validation bounds retry count and metadata; it is not authentication or a
// general payload-size limit.
package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/faustbrian/go-queue/v2/core"
)

// TaskFunc executes local work within one delivery timeout shared by all retries.
// It must honor cancellation; the scheduler cannot forcibly stop it.
type TaskFunc func(context.Context) error

var marshalJSON = json.Marshal

// DefaultMaxMessageBytes bounds encoded messages to one mebibyte.
const DefaultMaxMessageBytes = 1024 * 1024

// MaxRetryCount bounds additional handler retries per delivery; the first
// attempt and broker redelivery are separate.
const MaxRetryCount int64 = 100

var (
	// ErrInvalidMessageLimit reports a non-positive decode limit.
	ErrInvalidMessageLimit = errors.New("message byte limit must be positive")
	// ErrMessageTooLarge reports an encoded message above its decode limit.
	ErrMessageTooLarge = errors.New("message exceeds byte limit")
	// ErrInvalidMessage reports unsafe or inconsistent execution metadata.
	ErrInvalidMessage = errors.New("invalid message execution metadata")
)

// Message is a mutable execution envelope. Do not modify it concurrently with
// encoding or execution: Queue consumes RetryCount during retries, Payload aliases
// Body, and settlement setters do not synchronize or enforce callback idempotency.
type Message struct {
	// Task is an in-process callback omitted from the wire envelope.
	Task        TaskFunc `json:"-" msgpack:"-"`
	ack         func() error
	nack        func() error
	nackFailure func(error) error

	// Timeout is the positive total handler/retry budget; constructors default
	// to 60 minutes. Timeout signals cancellation, not forced completion.
	Timeout time.Duration `json:"timeout" msgpack:"timeout"`

	// Body is borrowed application payload, base64-encoded in JSON.
	Body []byte `json:"body" msgpack:"body"`

	// RetryCount is remaining additional handler attempts, from zero to 100.
	// The scheduler decrements it; broker redelivery counts are separate.
	RetryCount int64 `json:"retry_count" msgpack:"retry_count"`

	// RetryDelay is a nonnegative fixed retry delay; default zero selects backoff.
	RetryDelay time.Duration `json:"retry_delay" msgpack:"retry_delay"`

	// RetryFactor is the multiplying factor for each increment step.
	//
	// Defaults to 2.
	RetryFactor float64 `json:"retry_factor" msgpack:"retry_factor"`

	// RetryMin is the initial backoff duration when RetryDelay is zero.
	//
	// Defaults to 100 milliseconds.
	RetryMin time.Duration `json:"retry_min" msgpack:"retry_min"`

	// RetryMax caps backoff durations when RetryDelay is zero.
	//
	// Defaults to 10 seconds.
	RetryMax time.Duration `json:"retry_max" msgpack:"retry_max"`

	// Jitter eases contention by randomizing backoff steps
	Jitter bool `json:"jitter" msgpack:"jitter"`

	// Metadata is optional bounded identity retained for operational records.
	Metadata *Metadata `json:"metadata,omitempty" msgpack:"metadata,omitempty"`
}

// SetAcknowledgement attaches backend delivery settlement callbacks.
func (m *Message) SetAcknowledgement(ack func() error, nack func() error) {
	m.ack = ack
	m.nack = nack
	m.nackFailure = nil
}

// SetFailureAcknowledgement attaches settlement callbacks to a delivery whose
// backend needs the handler failure to choose retry or terminal behavior.
func (m *Message) SetFailureAcknowledgement(
	ack func() error,
	nack func(error) error,
) {
	m.ack = ack
	m.nack = nil
	m.nackFailure = nack
}

// AcknowledgementRequired reports whether backend settlement is attached.
func (m *Message) AcknowledgementRequired() bool {
	return m.ack != nil || m.nack != nil || m.nackFailure != nil
}

// Ack acknowledges successful processing when the backend requires it.
func (m *Message) Ack() error {
	if m.ack == nil {
		return nil
	}
	return m.ack()
}

// Nack rejects unsuccessful processing when the backend requires it.
func (m *Message) Nack() error {
	if m.nack != nil {
		return m.nack()
	}
	if m.nackFailure != nil {
		return m.nackFailure(nil)
	}

	return nil
}

// NackFailure rejects unsuccessful processing with its classified cause.
func (m *Message) NackFailure(err error) error {
	if m.nackFailure == nil {
		return m.Nack()
	}

	return m.nackFailure(err)
}

// Validate checks positive Timeout, RetryCount in [0, MaxRetryCount],
// nonnegative RetryDelay and bounded metadata. Backoff factor/min/max are checked
// only when retries are enabled without a fixed delay. Body size and the maximum
// timeout are not checked. The receiver must be non-nil.
func (m *Message) Validate() error {
	if m.Metadata != nil {
		if err := m.Metadata.Validate(); err != nil {
			return err
		}
	}
	if m.Timeout <= 0 {
		return fmt.Errorf("%w: timeout must be positive", ErrInvalidMessage)
	}
	if m.RetryCount < 0 || m.RetryCount > MaxRetryCount {
		return fmt.Errorf(
			"%w: retry count must be between 0 and %d",
			ErrInvalidMessage,
			MaxRetryCount,
		)
	}
	if m.RetryDelay < 0 {
		return fmt.Errorf("%w: retry delay cannot be negative", ErrInvalidMessage)
	}
	if m.RetryCount == 0 || m.RetryDelay > 0 {
		return nil
	}
	if math.IsNaN(m.RetryFactor) || math.IsInf(m.RetryFactor, 0) || m.RetryFactor < 1 {
		return fmt.Errorf("%w: retry factor must be finite and at least one", ErrInvalidMessage)
	}
	if m.RetryMin <= 0 {
		return fmt.Errorf("%w: retry minimum must be positive", ErrInvalidMessage)
	}
	if m.RetryMax < m.RetryMin {
		return fmt.Errorf("%w: retry maximum must not be below minimum", ErrInvalidMessage)
	}

	return nil
}

// Payload returns Body without copying. The caller must coordinate any mutation
// with the owner of the message and its backing array.
func (m *Message) Payload() []byte {
	return m.Body
}

// CorrelationMetadata returns a copy of the transport-neutral carrier attached
// by a correlation-aware producer.
func (m *Message) CorrelationMetadata() map[string]string {
	if m == nil || m.Metadata == nil || m.Metadata.Correlation == nil {
		return nil
	}
	metadata := make(map[string]string, len(m.Metadata.Correlation))
	for key, value := range m.Metadata.Correlation {
		metadata[key] = value
	}

	return metadata
}

// TraceContextMetadata returns a copy of the transport-neutral telemetry
// carrier attached by a trace-aware producer.
func (m *Message) TraceContextMetadata() map[string]string {
	if m == nil || m.Metadata == nil || m.Metadata.TraceContext == nil {
		return nil
	}
	metadata := make(map[string]string, len(m.Metadata.TraceContext))
	for key, value := range m.Metadata.TraceContext {
		metadata[key] = value
	}

	return metadata
}

// Bytes returns a newly allocated JSON envelope and panics on marshal failure.
// It does not call Validate or apply a byte limit. Task and callbacks are omitted.
func (m *Message) Bytes() []byte {
	b, err := marshalJSON(m)
	if err != nil {
		panic(err)
	}

	return b
}

// NewMessage uses only the first AllowOption and borrows m.Bytes() as Body
// without another copy. Metadata maps and time are cloned by NewOptions. It does
// not validate options; it uses m.Bytes() verbatim and m must be non-nil.
func NewMessage(m core.QueuedMessage, opts ...AllowOption) Message {
	o := NewOptions(opts...)

	return Message{
		RetryCount:  o.retryCount,
		RetryDelay:  o.retryDelay,
		RetryFactor: o.retryFactor,
		RetryMin:    o.retryMin,
		RetryMax:    o.retryMax,
		Jitter:      o.jitter,
		Timeout:     o.timeout,
		Body:        m.Bytes(),
		Metadata:    o.metadata,
	}
}

// NewTask creates a local, unserialized callback with defaults and only the
// first AllowOption. It does not validate options or start work.
func NewTask(task TaskFunc, opts ...AllowOption) Message {
	o := NewOptions(opts...)

	return Message{
		Timeout:     o.timeout,
		RetryCount:  o.retryCount,
		RetryDelay:  o.retryDelay,
		RetryFactor: o.retryFactor,
		RetryMin:    o.retryMin,
		RetryMax:    o.retryMax,
		Jitter:      o.jitter,
		Task:        task,
		Metadata:    o.metadata,
	}
}

// Encode returns a newly allocated JSON envelope, without validation or a size
// limit, and panics on marshal failure. A nil message encodes as JSON null.
func Encode(m *Message) []byte {
	b, err := marshalJSON(m)
	if err != nil {
		panic(err)
	}

	return b
}

// Decode decodes a JSON envelope with DefaultMaxMessageBytes and panics on error.
// Use DecodeE for an error-returning boundary.
func Decode(b []byte) *Message {
	msg, err := DecodeE(b, DefaultMaxMessageBytes)
	if err != nil {
		panic(err)
	}

	return msg
}

// DecodeE rejects a nonpositive byte limit or oversized input before JSON
// unmarshaling, then validates execution metadata. Unknown fields and duplicate
// keys follow encoding/json behavior; no package-specific nesting limit is added.
// Decoded payload bytes are owned by the returned message.
func DecodeE(b []byte, maxBytes int) (*Message, error) {
	if maxBytes <= 0 {
		return nil, ErrInvalidMessageLimit
	}
	if len(b) > maxBytes {
		return nil, fmt.Errorf("%w: got %d bytes, limit %d", ErrMessageTooLarge, len(b), maxBytes)
	}

	var msg Message
	if err := json.Unmarshal(b, &msg); err != nil {
		return nil, fmt.Errorf("decode message: %w", err)
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}

	return &msg, nil
}

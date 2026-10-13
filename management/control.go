package management

import (
	"context"
	"strings"
	"time"
)

const (
	// MaxReasonBytes bounds actor-supplied administrative reasons.
	MaxReasonBytes = 1_024
	// MaxBulkSelection bounds one destructive bulk-retry command.
	MaxBulkSelection uint32 = 1_000
)

// CommandAction identifies a data-plane management operation.
type CommandAction string

const (
	// CommandPause requests intake suspension without ending already running work.
	CommandPause CommandAction = "pause"
	// CommandResume requests active intake after a supported paused/draining state.
	CommandResume CommandAction = "resume"
	// CommandDrain requests intake closure and in-flight completion.
	CommandDrain CommandAction = "drain"
	// CommandTerminate requests irreversible terminating state and drain.
	CommandTerminate CommandAction = "terminate"
	// CommandRetry requests one failure/dead-letter retry under backend policy.
	CommandRetry CommandAction = "retry"
	// CommandBulkRetry requests a confirmed bounded backend-owned selection.
	CommandBulkRetry CommandAction = "bulk_retry"
	// CommandDelete requests removal of one administrative record.
	CommandDelete CommandAction = "delete"
	// CommandPurge requests confirmed removal of a supported target; queue support is provider-specific.
	CommandPurge CommandAction = "purge"
	// CommandReplay requests confirmed allowlisted replay with explicit duplicate policy.
	CommandReplay CommandAction = "replay"
)

// TargetKind identifies the data-plane resource affected by a command.
type TargetKind string

const (
	// TargetQueue scopes an operation to a logical queue.
	TargetQueue TargetKind = "queue"
	// TargetWorker scopes an operation to one worker identity.
	TargetWorker TargetKind = "worker"
	// TargetWorkerGroup scopes an operation to one worker group.
	TargetWorkerGroup TargetKind = "worker_group"
	// TargetFailure selects failed-attempt records.
	TargetFailure TargetKind = "failure"
	// TargetDeadLetter selects terminal records.
	TargetDeadLetter TargetKind = "dead_letter"
)

// Target identifies a resource without exposing backend addressing.
type Target struct {
	// Kind selects the supported resource or record category.
	Kind TargetKind
	// Name is the logical target identity, bounded by MaxIdentityBytes.
	Name string
}

// ReplayPolicy declares how an explicit replay destination handles duplicate
// job identities. It does not claim exactly-once execution.
type ReplayPolicy string

const (
	// ReplayRejectDuplicate rejects a previously registered replay within backend retention.
	ReplayRejectDuplicate ReplayPolicy = "reject_duplicate"
	// ReplayReplaceDuplicate replaces a registered queued replay, not prior application effects.
	ReplayReplaceDuplicate ReplayPolicy = "replace_duplicate"
)

// ReplayOptions contains the mandatory data-plane replay safeguards.
type ReplayOptions struct {
	// Destination must identify an explicitly allowed logical queue/stream.
	Destination string
	// IdempotencyPolicy selects replacement or rejection within the backend's retention/window semantics.
	IdempotencyPolicy ReplayPolicy
}

// Selection bounds a backend-owned bulk administrative operation. Adapters
// must apply their own deterministic selection semantics within this limit.
type Selection struct {
	// Limit is the positive requested item ceiling, checked against the applicable limit.
	Limit uint32
}

// Command is the stable envelope enforced by a queue management adapter.
type Command struct {
	// ID correlates one command and its returned result.
	ID string
	// IdempotencyKey selects provider-owned duplicate handling; shape validation supplies no durable deduplication.
	IdempotencyKey string
	// Actor is a caller assertion; authenticate it outside this envelope.
	Actor string
	// Reason is required bounded operator text, not automatically redacted.
	Reason string
	// Protocol is the management protocol version; zero is unknown or invalid.
	Protocol ProtocolVersion
	// Action selects the requested management operation.
	Action CommandAction
	// Target scopes the operation to one logical resource.
	Target Target
	// RequestedAt is the nonzero caller request time.
	RequestedAt time.Time
	// Deadline must follow RequestedAt; Validate does not compare it with the current clock.
	Deadline time.Time
	// Confirmed is required for purge, bulk retry and replay; it is not an access credential.
	Confirmed bool
	// Selection is retained mutable bulk selection; do not change it while a command is in use.
	Selection *Selection
	// Replay is retained mutable replay configuration; providers may retain it for deduplication.
	Replay *ReplayOptions
}

// Validate rejects malformed, unsupported, or unsafe control commands.
func (c Command) Validate() error {
	if invalidIdentity(c.ID) {
		return invalid("id", "is required and must be bounded")
	}
	if invalidIdentity(c.IdempotencyKey) {
		return invalid("idempotency_key", "is required and must be bounded")
	}
	if invalidIdentity(c.Actor) {
		return invalid("actor", "is required and must be bounded")
	}
	if strings.TrimSpace(c.Reason) == "" || len(c.Reason) > MaxReasonBytes {
		return invalid("reason", "is required and must be bounded")
	}
	if c.Protocol == (ProtocolVersion{}) {
		return invalid("protocol", "is required")
	}
	if !c.Action.valid() {
		return invalid("action", "is unsupported")
	}
	if !c.Target.Kind.valid() {
		return invalid("target.kind", "is unsupported")
	}
	if invalidIdentity(c.Target.Name) {
		return invalid("target.name", "is required and must be bounded")
	}
	if c.RequestedAt.IsZero() {
		return invalid("requested_at", "is required")
	}
	if !c.Deadline.After(c.RequestedAt) {
		return invalid("deadline", "must follow requested_at")
	}
	if !c.Action.supports(c.Target.Kind) {
		return invalid("target.kind", "is unsupported for the action")
	}

	switch c.Action {
	case CommandPurge:
		if !c.Confirmed {
			return invalid("confirmed", "is required for purge")
		}
	case CommandBulkRetry:
		if !c.Confirmed {
			return invalid("confirmed", "is required for bulk retry")
		}
		if c.Selection == nil {
			return invalid("selection", "is required")
		}
		if c.Selection.Limit == 0 || c.Selection.Limit > MaxBulkSelection {
			return invalid("selection.limit", "must be within the bulk limit")
		}
	case CommandReplay:
		if !c.Confirmed {
			return invalid("confirmed", "is required for replay")
		}
		if c.Replay == nil {
			return invalid("replay", "is required")
		}
		if invalidIdentity(c.Replay.Destination) {
			return invalid("replay.destination", "is required and must be bounded")
		}
		if !c.Replay.IdempotencyPolicy.valid() {
			return invalid("replay.idempotency_policy", "is unsupported")
		}
	default:
	}

	return nil
}

func (a CommandAction) valid() bool {
	switch a {
	case CommandPause,
		CommandResume,
		CommandDrain,
		CommandTerminate,
		CommandRetry,
		CommandBulkRetry,
		CommandDelete,
		CommandPurge,
		CommandReplay:
		return true
	default:
		return false
	}
}

func (a CommandAction) supports(target TargetKind) bool {
	switch a {
	case CommandPause, CommandResume:
		return target == TargetQueue || target == TargetWorkerGroup
	case CommandDrain, CommandTerminate:
		return target == TargetWorker || target == TargetWorkerGroup
	case CommandRetry, CommandBulkRetry, CommandDelete, CommandReplay:
		return target == TargetFailure || target == TargetDeadLetter
	case CommandPurge:
		return target == TargetQueue || target == TargetFailure || target == TargetDeadLetter
	default:
		return false
	}
}

func (k TargetKind) valid() bool {
	switch k {
	case TargetQueue, TargetWorker, TargetWorkerGroup, TargetFailure, TargetDeadLetter:
		return true
	default:
		return false
	}
}

func (p ReplayPolicy) valid() bool {
	switch p {
	case ReplayRejectDuplicate, ReplayReplaceDuplicate:
		return true
	default:
		return false
	}
}

// CommandResultStatus describes an acknowledged or safely unknown outcome.
type CommandResultStatus string

const (
	// CommandAcknowledged reports provider-confirmed command completion, not handler execution.
	CommandAcknowledged CommandResultStatus = "acknowledged"
	// CommandRejected reports a policy or input refusal.
	CommandRejected CommandResultStatus = "rejected"
	// CommandFailed reports a known operation failure.
	CommandFailed CommandResultStatus = "failed"
	// CommandUnsupported reports absent protocol/action capability.
	CommandUnsupported CommandResultStatus = "unsupported"
	// CommandTimedOut reports an expired operation budget; applied effects may remain.
	CommandTimedOut CommandResultStatus = "timed_out"
	// CommandPartial reports mixed or incompletely reconciled effects.
	CommandPartial CommandResultStatus = "partial"
	// CommandUnknown reports an outcome the provider cannot determine.
	CommandUnknown CommandResultStatus = "unknown"
)

// CommandResult is the redacted data-plane acknowledgement for one command.
// FailureCode must be a stable code and must never contain payloads,
// credentials, tokens, or backend endpoints.
type CommandResult struct {
	// CommandID echoes the command identity.
	CommandID string
	// IdempotencyKey echoes the duplicate-operation key.
	IdempotencyKey string
	// WorkerID identifies the reporting worker or aggregate.
	WorkerID string
	// Protocol is the management protocol version; zero is unknown or invalid.
	Protocol ProtocolVersion
	// Status distinguishes acknowledged, rejected, partial and unknown outcomes.
	Status CommandResultStatus
	// FailureCode is an operational failure label, not arbitrary error text.
	FailureCode string
	// CompletedAt is the nonzero reporting time, not proof of durable handler completion.
	CompletedAt time.Time
}

// Validate checks result identity, protocol, status and nonzero completion time.
// FailureCode is shape-checked only for statuses requiring it; no content
// redaction or durable effect reconciliation is performed.
func (r CommandResult) Validate() error {
	if invalidIdentity(r.CommandID) {
		return invalid("command_id", "is required and must be bounded")
	}
	if invalidIdentity(r.IdempotencyKey) {
		return invalid("idempotency_key", "is required and must be bounded")
	}
	if invalidIdentity(r.WorkerID) {
		return invalid("worker_id", "is required and must be bounded")
	}
	if r.Protocol == (ProtocolVersion{}) {
		return invalid("protocol", "is required")
	}
	if !r.Status.valid() {
		return invalid("status", "is unsupported")
	}
	if r.CompletedAt.IsZero() {
		return invalid("completed_at", "is required")
	}
	if r.Status.requiresFailureCode() && invalidIdentity(r.FailureCode) {
		return invalid("failure_code", "is required and must be bounded")
	}

	return nil
}

func (s CommandResultStatus) valid() bool {
	switch s {
	case CommandAcknowledged,
		CommandRejected,
		CommandFailed,
		CommandUnsupported,
		CommandTimedOut,
		CommandPartial,
		CommandUnknown:
		return true
	default:
		return false
	}
}

func (s CommandResultStatus) requiresFailureCode() bool {
	switch s {
	case CommandRejected, CommandFailed, CommandUnsupported, CommandTimedOut, CommandPartial:
		return true
	case CommandAcknowledged, CommandUnknown:
		return false
	default:
		return true
	}
}

// Controller is implemented by queue adapters that can safely enforce
// management commands. A control plane depends on this interface rather than
// issuing backend commands directly.
type Controller interface {
	// Execute applies a command under provider-owned deduplication/outcome rules.
	// Authorization remains caller-owned.
	Execute(context.Context, Command) (CommandResult, error)
}

package management

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MaxDesiredStateTargets bounds one reconciler's copied target set.
const MaxDesiredStateTargets = 256

var (
	// ErrDesiredStateNotFound reports a target without an authored state. A
	// worker keeps its current local state and does not infer an active state.
	ErrDesiredStateNotFound = errors.New("management: desired state not found")
	// ErrInvalidDesiredStateConfiguration reports an unsafe reconciler graph.
	ErrInvalidDesiredStateConfiguration = errors.New("management: invalid desired state configuration")
	// ErrInvalidDesiredStateContext reports a nil reconciliation context.
	ErrInvalidDesiredStateContext = errors.New("management: invalid desired state context")
	// ErrInvalidDesiredStateOutput reports malformed or mismatched source data.
	ErrInvalidDesiredStateOutput = errors.New("management: invalid desired state output")
	// ErrDesiredStateRegression prevents an older revision from replacing state.
	ErrDesiredStateRegression = errors.New("management: desired state revision regression")
	// ErrDesiredStateConflict prevents one revision from describing two states.
	ErrDesiredStateConflict = errors.New("management: desired state revision conflict")
)

// DesiredState is the durable lifecycle state authored by a control plane.
type DesiredState string

const (
	// DesiredActive permits local intake.
	DesiredActive DesiredState = "active"
	// DesiredPaused stops new admissions and waits for admission reservations, not active jobs.
	DesiredPaused DesiredState = "paused"
	// DesiredDraining stops intake and waits for admissions and jobs.
	DesiredDraining DesiredState = "draining"
	// DesiredTerminating irreversibly stops intake and waits for drain.
	DesiredTerminating DesiredState = "terminating"
)

// DesiredRecord is one attributed, monotonic target-state revision.
type DesiredRecord struct {
	// Target scopes the operation to one logical resource.
	Target Target
	// State is the reported or requested lifecycle state, not transport connectivity proof.
	State DesiredState
	// Revision is a positive target-local revision; regressions are rejected by the reconciler.
	Revision uint64
	// ChangedAt is the nonzero authored time, not an ordering substitute for Revision.
	ChangedAt time.Time
	// CommandID attributes the desired revision to a bounded command identity.
	CommandID string
}

// Validate checks revision, target, state, command identity and time shape.
// It does not authenticate authorship or establish durability.
func (r DesiredRecord) Validate() error {
	if !desiredTarget(r.Target) {
		return invalid("target", "is unsupported or invalid")
	}
	if !r.State.valid() {
		return invalid("state", "is unsupported")
	}
	if r.Revision == 0 {
		return invalid("revision", "must be positive")
	}
	if r.ChangedAt.IsZero() {
		return invalid("changed_at", "is required")
	}
	if invalidIdentity(r.CommandID) {
		return invalid("command_id", "is required and must be bounded")
	}

	return nil
}

// DesiredStateReader reads authoritative tenant-scoped desired state.
type DesiredStateReader interface {
	// GetDesiredState returns target-scoped authoritative state or ErrDesiredStateNotFound.
	GetDesiredState(context.Context, Target) (DesiredRecord, error)
}

// DesiredStateApplier changes local queue admission or lifecycle state.
type DesiredStateApplier interface {
	// ApplyDesiredState applies a revision; a returned error need not undo effects already applied.
	ApplyDesiredState(context.Context, DesiredRecord) error
}

// DesiredStateReconcilerConfig defines one bounded pull reconciliation set.
type DesiredStateReconcilerConfig struct {
	// Reader is a borrowed authoritative provider called under the reconciler mutex.
	Reader DesiredStateReader
	// Applier is a borrowed synchronous callback called under the reconciler mutex.
	Applier DesiredStateApplier
	// Targets is copied at construction and applied in configured order; duplicates are rejected.
	Targets []Target
}

// DesiredStateReconciler remembers successfully applied revisions per target in
// this process only. An Applier that fails after a side effect can be called again;
// this is not durable exactly-once application. Scheduling/retry remain caller-owned.
// The mutex serializes complete reconciliation, including Reader and Applier calls.
// Those callbacks must not reenter Reconcile, and must honor their contexts.
type DesiredStateReconciler struct {
	mu      sync.Mutex
	reader  DesiredStateReader
	applier DesiredStateApplier
	targets []Target
	applied map[Target]DesiredRecord
}

// NewDesiredStateReconciler creates a bounded revision-aware reconciler.
func NewDesiredStateReconciler(
	config DesiredStateReconcilerConfig,
) (*DesiredStateReconciler, error) {
	if nilStatusProvider(config.Reader) || nilStatusProvider(config.Applier) ||
		len(config.Targets) == 0 || len(config.Targets) > MaxDesiredStateTargets {
		return nil, ErrInvalidDesiredStateConfiguration
	}
	targets := append([]Target(nil), config.Targets...)
	seen := make(map[Target]struct{}, len(targets))
	for _, target := range targets {
		if !desiredTarget(target) {
			return nil, ErrInvalidDesiredStateConfiguration
		}
		if _, duplicate := seen[target]; duplicate {
			return nil, ErrInvalidDesiredStateConfiguration
		}
		seen[target] = struct{}{}
	}

	return &DesiredStateReconciler{
		reader: config.Reader, applier: config.Applier, targets: targets,
		applied: make(map[Target]DesiredRecord, len(targets)),
	}, nil
}

// Reconcile reads and applies targets in configured order, holding its mutex
// through all provider calls. Waiting for the mutex is not context-aware.
// Successfully applied revisions remain committed when a later target fails.
func (r *DesiredStateReconciler) Reconcile(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidDesiredStateContext
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, target := range r.targets {
		record, err := r.reader.GetDesiredState(ctx, target)
		if errors.Is(err, ErrDesiredStateNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if record.Validate() != nil || record.Target != target {
			return ErrInvalidDesiredStateOutput
		}
		current, exists := r.applied[target]
		if exists && record.Revision < current.Revision {
			return ErrDesiredStateRegression
		}
		if exists && record.Revision == current.Revision {
			if record != current {
				return ErrDesiredStateConflict
			}
			continue
		}
		if err := r.applier.ApplyDesiredState(ctx, record); err != nil {
			return err
		}
		r.applied[target] = record
	}

	return nil
}

func desiredTarget(target Target) bool {
	if invalidIdentity(target.Name) {
		return false
	}
	switch target.Kind {
	case TargetQueue, TargetWorker, TargetWorkerGroup:
		return true
	default:
		return false
	}
}

func (s DesiredState) valid() bool {
	switch s {
	case DesiredActive, DesiredPaused, DesiredDraining, DesiredTerminating:
		return true
	default:
		return false
	}
}

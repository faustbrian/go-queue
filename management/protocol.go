// Package management defines stable worker/control-plane contracts without
// implementing queue delivery or backend operations. DTO validation checks
// shape/bounds, not authentication, privacy or durability. Mutable slices/maps
// and optional pointers remain caller/provider-owned unless a constructor
// explicitly copies them. Capability claims are not authorization grants.
package management

import "slices"

// ProtocolVersion identifies a worker/control-plane management protocol.
type ProtocolVersion struct {
	// Major orders versions before Minor.
	Major uint16
	// Minor orders versions within one Major.
	Minor uint16
}

// ProtocolRange is an inclusive set of protocol versions supported during a
// rolling upgrade.
type ProtocolRange struct {
	// Minimum is the inclusive lower supported version.
	Minimum ProtocolVersion
	// Maximum is the inclusive upper supported version; reversed ranges negotiate unknown.
	Maximum ProtocolVersion
}

// Capability identifies an optional management or visibility operation.
// Capability claims are not authorization grants.
type Capability string

const (
	// CapabilityWorkerStatus advertises optional worker observations.
	CapabilityWorkerStatus Capability = "worker_status"
	// CapabilityQueueStatus advertises optional queue observations.
	CapabilityQueueStatus Capability = "queue_status"
	// CapabilityPause advertises optional intake suspension.
	CapabilityPause Capability = "pause"
	// CapabilityResume advertises optional intake resumption.
	CapabilityResume Capability = "resume"
	// CapabilityDrain advertises optional graceful drain.
	CapabilityDrain Capability = "drain"
	// CapabilityTerminate advertises optional termination.
	CapabilityTerminate Capability = "terminate"
	// CapabilityFailures advertises optional failure listing/inspection.
	CapabilityFailures Capability = "failures"
	// CapabilityDeadLetters advertises optional terminal listing/inspection.
	CapabilityDeadLetters Capability = "dead_letters"
	// CapabilityRetry advertises optional single-record retry.
	CapabilityRetry Capability = "retry"
	// CapabilityBulkRetry advertises optional bounded selected retry.
	CapabilityBulkRetry Capability = "bulk_retry"
	// CapabilityDelete advertises optional record deletion.
	CapabilityDelete Capability = "delete"
	// CapabilityPurge advertises optional target purge.
	CapabilityPurge Capability = "purge"
	// CapabilityReplay advertises optional explicit administrative replay.
	CapabilityReplay Capability = "replay"
	// CapabilityRetentionCount advertises optional maximum-count retention.
	CapabilityRetentionCount Capability = "retention_count"
	// CapabilityRetentionTime advertises optional time retention.
	CapabilityRetentionTime Capability = "retention_time"
	// CapabilityRetentionBytes advertises optional byte retention.
	CapabilityRetentionBytes Capability = "retention_bytes"
)

// CompatibilityState describes a worker relative to a supported protocol
// range.
type CompatibilityState string

const (
	// CompatibilityCompatible means the version falls within the inclusive range.
	CompatibilityCompatible CompatibilityState = "compatible"
	// CompatibilityWorkerOlder means the worker version is below the supported minimum.
	CompatibilityWorkerOlder CompatibilityState = "worker_older"
	// CompatibilityWorkerNewer means the worker version is above the supported maximum.
	CompatibilityWorkerNewer CompatibilityState = "worker_newer"
	// CompatibilityUnknown means a reversed range or zero worker version prevents negotiation.
	CompatibilityUnknown CompatibilityState = "unknown"
)

// Compatibility is the deterministic result of version and capability
// negotiation.
type Compatibility struct {
	// State classifies version compatibility, independently of authentication.
	State CompatibilityState
	// Enabled is a sorted, deduplicated intersection only for compatible peers.
	Enabled []Capability
	// WorkerOnly contains sorted worker-only capability claims.
	WorkerOnly []Capability
	// ControlPlaneOnly contains sorted control-plane-only capability claims.
	ControlPlaneOnly []Capability
}

// Negotiate enables only capabilities both peers report at a compatible
// protocol version. Unknown and incompatible peers remain visible but are not
// controllable.
func Negotiate(
	supported ProtocolRange,
	worker ProtocolVersion,
	workerCapabilities []Capability,
	controlPlaneCapabilities []Capability,
) Compatibility {
	state := classifyCompatibility(supported, worker)
	workerSet := capabilitySet(workerCapabilities)
	controlPlaneSet := capabilitySet(controlPlaneCapabilities)

	result := Compatibility{State: state}
	for capability := range workerSet {
		if _, supportedByControlPlane := controlPlaneSet[capability]; supportedByControlPlane {
			if state == CompatibilityCompatible {
				result.Enabled = append(result.Enabled, capability)
			}

			continue
		}

		result.WorkerOnly = append(result.WorkerOnly, capability)
	}
	for capability := range controlPlaneSet {
		if _, supportedByWorker := workerSet[capability]; !supportedByWorker {
			result.ControlPlaneOnly = append(result.ControlPlaneOnly, capability)
		}
	}

	sortCapabilities(result.Enabled)
	sortCapabilities(result.WorkerOnly)
	sortCapabilities(result.ControlPlaneOnly)

	return result
}

func classifyCompatibility(supported ProtocolRange, worker ProtocolVersion) CompatibilityState {
	if compareVersion(supported.Minimum, supported.Maximum) > 0 || worker == (ProtocolVersion{}) {
		return CompatibilityUnknown
	}
	if compareVersion(worker, supported.Minimum) < 0 {
		return CompatibilityWorkerOlder
	}
	if compareVersion(worker, supported.Maximum) > 0 {
		return CompatibilityWorkerNewer
	}

	return CompatibilityCompatible
}

func compareVersion(left, right ProtocolVersion) int {
	if left.Major < right.Major {
		return -1
	}
	if left.Major > right.Major {
		return 1
	}
	if left.Minor < right.Minor {
		return -1
	}
	if left.Minor > right.Minor {
		return 1
	}

	return 0
}

func capabilitySet(capabilities []Capability) map[Capability]struct{} {
	set := make(map[Capability]struct{}, len(capabilities))
	for _, capability := range capabilities {
		set[capability] = struct{}{}
	}

	return set
}

func sortCapabilities(capabilities []Capability) {
	slices.Sort(capabilities)
}

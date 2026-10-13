// Package core defines backend worker, message and optional settlement seams.
// Implementations own transport lifetimes and byte ownership. Queue calls Run
// concurrently, Queue from submitters and Request from its intake loop; custom
// workers must synchronize the operations they permit and unblock intake on close.
package core

import (
	"context"
)

// Worker represents an interface for a worker that processes tasks.
// It provides methods to run tasks, shut down the worker, queue tasks, and request tasks from the queue.
type Worker interface {
	// Run handles one decoded task, potentially concurrently with other Run calls.
	// It must honor ctx; the scheduler cannot forcibly join noncooperative work.
	Run(ctx context.Context, task TaskMessage) error

	// Shutdown stops the worker and performs any necessary cleanup.
	// It must release owned intake/resources; this interface supplies no context
	// or timeout, so the implementation owns its shutdown bound.
	Shutdown() error

	// Queue adds a task to the worker's queue.
	// Returned success describes admission only; durability and ambiguous errors
	// are backend-specific. There is no caller context in this interface.
	Queue(task TaskMessage) error

	// Request retrieves a task from the worker's queue.
	// Implementations own blocking/timeout bounds and close behavior. A returned
	// message is not proof of settlement or completed application work.
	Request() (TaskMessage, error)
}

// WorkerMetadata identifies the backend and logical queue represented by a worker.
// Queue uses it to enrich lifecycle events without coupling to backend packages.
type WorkerMetadata interface {
	// BackendName supplies an operational backend label, not authenticated identity.
	BackendName() string
	// QueueName supplies the logical queue label; an unnamed Ring returns empty.
	QueueName() string
}

// DeliveryValidator is an additive worker extension for validating one
// decoded delivery before the root queue starts its handler retry loop.
type DeliveryValidator interface {
	// ValidateDelivery checks original decoded metadata once before handler retries.
	ValidateDelivery(TaskMessage) error
}

// QueuedMessage represents an interface for a message that can be queued.
// It requires the implementation of a Bytes method, which returns the message
// content as a slice of bytes.
type QueuedMessage interface {
	// Bytes returns the implementation-defined payload or wire envelope. Document
	// alias/copy lifetime; queue admission does not add an application-body copy.
	Bytes() []byte
}

// TaskMessage represents an interface for a task message that can be queued.
// It embeds the QueuedMessage interface and adds a method to retrieve the payload of the message.
type TaskMessage interface {
	QueuedMessage
	// Payload returns application bytes; ownership is implementation-defined.
	Payload() []byte
}

// Acknowledger is implemented by deliveries that require explicit settlement.
type Acknowledger interface {
	// AcknowledgementRequired distinguishes explicit settlement from transient work.
	AcknowledgementRequired() bool
	// Ack settles success; an error can leave a recoverable or ambiguous outcome.
	Ack() error
	// Nack settles failure under backend policy, not a guaranteed redelivery.
	Nack() error
}

// FailureAcknowledger is an additive settlement extension for backends that
// need the classified handler outcome to choose retry or terminal behavior.
// Implementations must preserve the source delivery when settlement fails.
type FailureAcknowledger interface {
	// NackFailure receives the final error to select backend settlement policy.
	NackFailure(error) error
}

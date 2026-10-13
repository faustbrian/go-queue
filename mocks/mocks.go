// Package mocks contains externally generated GoMock doubles for core interfaces.
// Headers record the commands but not the original generator version. The root
// runtime module is go.uber.org/mock v0.6.0; that version's mockgen is available,
// but its output differs from the inherited files. Do not infer original producer
// identity from the runtime pin or manually rewrite generated bodies. Commands
// run from this directory and correctly bind the unversioned root core path.
package mocks

//go:generate mockgen -package=mocks -destination=mock_worker.go github.com/faustbrian/go-queue/core Worker
//go:generate mockgen -package=mocks -destination=mock_queued_message.go github.com/faustbrian/go-queue/core QueuedMessage
//go:generate mockgen -package=mocks -destination=mock_task_message.go github.com/faustbrian/go-queue/core TaskMessage

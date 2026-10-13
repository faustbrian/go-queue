// Package mocks contains externally generated GoMock doubles for core interfaces.
// Current outputs use the explicitly selected MockGen v0.6.0 producer below;
// see README.md for authenticated inputs and reproduction. The inherited outputs'
// original producer remains unknown, independently of the runtime dependency pin.
// Do not manually edit generated bodies. Commands run from this directory and
// bind the unversioned root core path.
package mocks

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -package=mocks -destination=mock_worker.go github.com/faustbrian/go-queue/core Worker
//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -package=mocks -destination=mock_queued_message.go github.com/faustbrian/go-queue/core QueuedMessage
//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -package=mocks -destination=mock_task_message.go github.com/faustbrian/go-queue/core TaskMessage

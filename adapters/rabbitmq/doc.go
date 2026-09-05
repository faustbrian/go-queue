// Package rabbitmq adapts the backend-neutral go-queue worker contract to the
// RabbitMQ-native policy implemented by github.com/faustbrian/go-rabbitmq-queues.
//
// Applications that need exchanges, independent fan-out, queue-type-specific
// policy, native publications, or direct delivery settlement should use
// go-rabbitmq-queues instead. This module exists for bounded migration of
// go-queue job workers.
package rabbitmq

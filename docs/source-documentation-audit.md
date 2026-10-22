# Source documentation audit

[Documentation index](README.md)

This is a historical audit of the v1 source at the commit named below. Its
`rabbitmq`, `queueservice` and root `redisstream` rows describe paths that v2
removes.

## Initial scope and bounded plan

Tier A source comments and directly related current contract prose from
`2b3b8e44bbed087179ca53f0ede1276c63c25f1d`. This worker owns documentation;
the coordinator owns publication, exact-source CI and aggregate/enforcement.
The unrelated original checkout and its untracked work are preserved.

Reproduce using `git ls-files "*.go"`, `modules.json`, `packages.json`,
`golib source-docs check --json` and test-only entrypoint inventory. Scope is
205 tracked Go files: 68 hand-maintained implementation sources, three external
MockGen outputs and 134 assurance sources. Five modules are root,
`adapters/rabbitmq`, `adapters/service`, `rabbitmq` and `queueservice`; all five
API baselines use apidiff, not full prose snapshots. Twenty-five source
directories include public workers/control contracts, private semantics/helpers,
commands and examples. Inventory includes 714 Test-prefixed function declarations, 17 fuzz
targets, 28 benchmarks and 15 Example declarations (13 have output; two have no runnable output checks); counts are scope, not proof.

Outcome: truthful admission, worker/settlement ownership, delivery versus ack,
timeout/retry/dead-letter boundaries, management diagnostics and lifecycle
shutdown/drain contracts, including deliberately different providers and legacy
facades. Direct consumers include Queue/core Worker integrations, message/job
codecs, management/control transports, stream backends, RabbitMQ/service
adapters and the declared scheduler/service/outbox/webhook consumers.
Named risks: concurrent admission versus release, irreversible backend ack or
publication, cooperative cancellation, broker/store consistency, partial
control results, callback ownership, resource windows, transport diagnostics,
legacy wrapper compatibility and generated-mock producer trust.

Smallest checks: pinned Go1.27.2 all-source AST/token/literal/directive identity
and formatting, useful package renders/native API mode discovery, selected
native docs/examples, source/links/offline specification checks, pinned
full/added/staged secrets and one ordinary whole-diff review. Externally generated
mocks are not manually rewritten: inspect trusted producer availability and
compare original/current input output under the pinned available mock module before adjudicating the
three generator-review findings. Producer/input/output trust changes are outside
Tier A and require coordinator-owned independent review. No live brokers, Docker,
Kind, fuzz/race/mutation/benchmark/interop/release campaigns. All Go/native runs
await an explicit serialized grant, with three owned caches and exact cleanup.

## Complete tracked inventory and initial gaps

Initial findings: 472 structural omissions plus three external generator reviews.
Missing prose is distinct from contracts/producer questions requiring source
investigation. Existing generation, integration build and deprecation directives
must remain unchanged. The generated headers/directives currently name
`github.com/faustbrian/go-queue/core`, correctly matching the unversioned root
module. `go.uber.org/mock` is pinned to v0.6.0; no producer has been executed yet.

| Go file | Initial findings | Source boundary |
| --- | --- | --- |
| `aaa_queue_contract_test.go` | 0 | TestWithWorkerCountPreservesPositiveCount; TestQueueShutdownClosesIdleRing; additional cases/helpers |
| `adapters/rabbitmq/aaa_mutation_boundaries_test.go` | 0 | TestValidateDeadLetterRejectsEachInvalidBoundary; TestDefaultOptionsPreserveOperationalBounds |
| `adapters/rabbitmq/adapter.go` | 5 | implementation contracts and private invariants |
| `adapters/rabbitmq/adapter_boundary_test.go` | 0 | TestAdapterRejectsUnsupportedAndInvalidNativePolicies; TestAdapterRejectsInvalidExchangeWithoutLoggingCallerIdentity; additional cases/helpers |
| `adapters/rabbitmq/adapter_contract_test.go` | 0 | TestAdapterConfirmsRetryReplacementBeforeAcknowledgingSource; TestAdapterKeepsSourceRecoverableUnlessReplacementIsConfirmed; additional cases/helpers |
| `adapters/rabbitmq/benchmark_test.go` | 0 | BenchmarkAdapterConfirmedPublishOverhead |
| `adapters/rabbitmq/decoder_fuzz_test.go` | 0 | FuzzAdapterDeliveryAttempt |
| `adapters/rabbitmq/doc.go` | 0 | implementation contracts and private invariants |
| `adapters/rabbitmq/example_test.go` | 0 | ExampleNewWorkerE |
| `adapters/rabbitmq/integration_test.go` | 0 | TestLiveRabbitMQAdapterPolicy |
| `adapters/rabbitmq/leak_test.go` | 0 | TestMain |
| `adapters/rabbitmq/options.go` | 4 | implementation contracts and private invariants |
| `adapters/rabbitmq/options_test.go` | 0 | Test_isVaildExchange |
| `adapters/rabbitmq/rabbitmq.go` | 3 | implementation contracts and private invariants |
| `adapters/rabbitmq/successor_contract_test.go` | 0 | TestSuccessorExposesRabbitMQCompatibilityAPI |
| `adapters/redisstream/aaa_mutation_boundaries_test.go` | 0 | TestReplayDestinationsRejectsAnOversizedAllowlist; TestRedisOptionsPreserveInclusiveLimitsAndLaterDestinations; additional cases/helpers |
| `adapters/redisstream/api_compat_test.go` | 0 | TestRedisStreamPublicAPIDoesNotLeakNativeClients; TestRedisStreamStatsRemainSourceCompatible |
| `adapters/redisstream/benchmark_test.go` | 0 | BenchmarkRedisStreamAck; BenchmarkRedisStreamStats |
| `adapters/redisstream/conformance_test.go` | 0 | TestRedisStreamWorkerConformance |
| `adapters/redisstream/constructor_test.go` | 0 | TestNewWorkerEReturnsInvalidConnectionString; TestNewWorkerERequiresRedisAddress; additional cases/helpers |
| `adapters/redisstream/control.go` | 1 | implementation contracts and private invariants |
| `adapters/redisstream/control_fault_test.go` | 0 | TestRedisControllerExecutionBoundaries; TestRedisReplayRejectsMissingMalformedAndCapacityRecords; additional cases/helpers |
| `adapters/redisstream/control_test.go` | 0 | TestControllerRetriesDeadLetterIdempotentlyAfterDurableEnqueue; TestControllerBulkRetryDeleteAndPurgeStayBounded; additional cases/helpers |
| `adapters/redisstream/decoder_fuzz_test.go` | 0 | FuzzRequestDelivery |
| `adapters/redisstream/doc.go` | 0 | implementation contracts and private invariants |
| `adapters/redisstream/options.go` | 1 | implementation contracts and private invariants |
| `adapters/redisstream/records.go` | 0 | implementation contracts and private invariants |
| `adapters/redisstream/records_test.go` | 0 | TestRecordReaderListsAndInspectsRedisDeadLetters; TestRecordReaderBoundsPaginationFiltersAndMalformedData; additional cases/helpers |
| `adapters/redisstream/redis.go` | 5 | implementation contracts and private invariants |
| `adapters/redisstream/settlement_fault_test.go` | 0 | TestRedisMalformedSettlementKeepsSourceRecoverableAtEveryBoundary; TestRedisHandlerSettlementReportsFailureAndDeadLetterBoundaries; additional cases/helpers |
| `adapters/redisstream/settlement_test.go` | 0 | TestRequestDefersRedisStreamAcknowledgement |
| `adapters/redisstream/status.go` | 0 | implementation contracts and private invariants |
| `adapters/redisstream/status_test.go` | 0 | TestRedisStreamNativeManagementStatus; TestRedisGroupLagCapabilityRequiresRedisSeven; additional cases/helpers |
| `adapters/redisstream/successor_contract_test.go` | 0 | TestSuccessorExposesUnambiguousRedisStreamWorkerAPI |
| `adapters/redisstream/worker_test.go` | 0 | TestOptionsConfigureRedisStreams; TestWorkerRejectsUnsafeDeadLetterConfigurationBeforeConnecting; additional cases/helpers |
| `adapters/redisstream/zzz_integration_test.go` | 0 | TestMain; TestWithRedis; additional cases/helpers |
| `adapters/service/admission_test.go` | 0 | TestConcreteWorkerServiceDrainClosesQueueAdmissionBeforeShutdown; TestConcreteWorkerReleasePanicIsSecretSafeAndTerminal; additional cases/helpers |
| `adapters/service/api_compat_test.go` | 0 | implementation contracts and private invariants |
| `adapters/service/backend_integration_test.go` | 0 | TestDurableBackendsDrainThroughQueueService; TestDurableBackendAdaptersDisconnectAndReconnect |
| `adapters/service/benchmark_test.go` | 0 | BenchmarkProducerPublish; BenchmarkProducerDrain; additional cases/helpers |
| `adapters/service/callback_error_test.go` | 0 | TestCallbackFailuresPreserveCausesWithoutDisclosingTheirText; TestStartupErrorDiagnosticReportsWhetherCleanupAlsoFailed |
| `adapters/service/durable_lifecycle_integration_test.go` | 0 | TestDurableBackendHandlerTimeoutRedeliversAfterLeaseExpiry; TestDurableBackendDeadLetterFailureLeavesWorkForRecovery; additional cases/helpers |
| `adapters/service/durable_process_integration_test.go` | 0 | TestDurableBackendProcessTerminationDuplicateWindowAndRecovery; TestDurableBackendProcessHelper |
| `adapters/service/example_test.go` | 0 | ExampleNewWorker; ExampleNewLifecycleWorker |
| `adapters/service/fuzz_test.go` | 0 | FuzzTransportMetadataIsolation |
| `adapters/service/health_lifecycle_test.go` | 0 | TestHealthProbesRaceWorkerDrainAndCancellation; TestHealthProbesRaceWorkerBackendFailure |
| `adapters/service/lifecycle_contract_test.go` | 0 | TestProducerStartupReadinessAndShutdownLifecycle; TestSharedProducerCanRetryAfterStartupValidationFailure; additional cases/helpers |
| `adapters/service/lifecycle_edge_test.go` | 0 | TestProducerOptionalReadinessAndLifecycleCallbacks; TestProducerConcurrentStartAndStopSerializeOwnership; additional cases/helpers |
| `adapters/service/lifecycle_hardening_test.go` | 0 | TestProducerDrainAndShutdownShareOneTerminationBudget; TestLifecycleWorkerRacesDrainReadinessCancellationAndDuplicateSignals; additional cases/helpers |
| `adapters/service/mutation_contract_test.go` | 0 | TestStartupDiagnosticReportsWhetherRollbackAlsoFailed; TestSharedProducerCanRetryStartupBecauseNoResourceWasClosed; additional cases/helpers |
| `adapters/service/pod_termination_integration_test.go` | 0 | TestKubernetesPodTerminationDuplicateWindowAndRecovery |
| `adapters/service/process_termination_test.go` | 0 | TestProcessTerminationDuplicateWindowAndRecovery; TestQueueServiceProcessHelper |
| `adapters/service/queueservice.go` | 0 | implementation contracts and private invariants |
| `adapters/service/queueservice_test.go` | 0 | TestProducerPropagatesCorrelationAndHandlerCreatesDeliveryAttempt; TestProducerAndHandlerPropagateCallerOwnedTraceContext; additional cases/helpers |
| `adapters/service/shutdown_once_test.go` | 0 | TestFailedShutdownIsObservedWithoutClosingTheResourceAgain |
| `adapters/service/successor_contract_test.go` | 0 | TestSuccessorExposesQueueServiceLifecycleAPI |
| `automation_test.go` | 0 | TestSharedRepositoryContract; TestSharedDocumentationConventions; additional cases/helpers |
| `benchmark_test.go` | 0 | BenchmarkNewRing; BenchmarkQueueTask; additional cases/helpers |
| `cmd/semvercheck/main.go` | 1 | implementation contracts and private invariants |
| `cmd/semvercheck/main_test.go` | 0 | TestNextStableVersion |
| `core/worker.go` | 10 | implementation contracts and private invariants |
| `coverage_characterization_test.go` | 0 | TestDefaultLoggerWritesUnformattedErrorsAndFatals; TestEmptyLoggerAcceptsEveryLogShape; additional cases/helpers |
| `errors.go` | 1 | implementation contracts and private invariants |
| `examples/inmemory/main.go` | 1 | implementation contracts and private invariants |
| `examples/redis/main.go` | 1 | implementation contracts and private invariants |
| `examples/valkey/main.go` | 1 | implementation contracts and private invariants |
| `graceful_release_test.go` | 0 | TestReleaseContextStopsAdmissionAndDrainsHandlersBeforeShutdown; TestCloseAdmissionRejectsNewWorkWithoutReleasingWorker; additional cases/helpers |
| `idle_drain_test.go` | 0 | TestIdleDrainDoesNotWaitForRetryInterval; TestDrainRetainsTaskAlreadyWaitingAfterRequestError |
| `internal/safeerr/safeerr.go` | 0 | implementation contracts and private invariants |
| `internal/safeerr/safeerr_test.go` | 0 | TestWrapRedactsCauseTextAndPreservesIdentity |
| `internal/semver/semver.go` | 3 | implementation contracts and private invariants |
| `internal/semver/semver_test.go` | 0 | TestValidateTag; TestNextStable |
| `internal/streamqueue/metadata.go` | 0 | implementation contracts and private invariants |
| `internal/streamqueue/metadata_test.go` | 0 | TestMessageMetadataAppliesValidatedAllowlistWithoutAliasing; TestMessageMetadataLeavesUnknownAndMalformedValuesAlone |
| `internal/streamqueue/semantics.go` | 51 | implementation contracts and private invariants |
| `internal/streamqueue/semantics_test.go` | 0 | TestSemanticRequestsValidateBounds; TestReadAndClaimRequestsAcceptMaximumBatchSize; additional cases/helpers |
| `internal/testutil/apiguard/apiguard.go` | 0 | implementation contracts and private invariants |
| `internal/testutil/apiguard/apiguard_test.go` | 0 | TestPackageReferencesFindsOnlyExportedNativeTypes; TestPackageReferencesRejectsMalformedSource |
| `internal/testutil/streamconformance/conformance.go` | 0 | implementation contracts and private invariants |
| `job/benchmark_test.go` | 0 | BenchmarkNewTask; BenchmarkNewMessage; additional cases/helpers |
| `job/correlation_test.go` | 0 | TestCorrelationMetadataIsClonedValidatedAndEncoded; TestCorrelationMetadataRejectsUnboundedCarriers; additional cases/helpers |
| `job/job.go` | 6 | implementation contracts and private invariants |
| `job/job_fuzz_test.go` | 0 | FuzzDecodeE; FuzzMessageValidation |
| `job/job_test.go` | 0 | TestMessageEncodeDecode; TestTaskCarriesAllExecutionOptions; additional cases/helpers |
| `job/metadata.go` | 13 | implementation contracts and private invariants |
| `job/option.go` | 8 | implementation contracts and private invariants |
| `job/option_test.go` | 0 | TestOptions |
| `lifecycle_hardening_test.go` | 0 | TestReleaseBeforeStartDrainsRing; TestStartIsIdempotent; additional cases/helpers |
| `logger.go` | 0 | implementation contracts and private invariants |
| `logger_test.go` | 0 | TestNewLoggerUsesDateAndTimeForEveryLevel; ExampleNewEmptyLogger |
| `management/classification.go` | 4 | implementation contracts and private invariants |
| `management/classification_test.go` | 0 | TestClassifyFailureUsesStablePrecedenceAndPreservesCauses; TestOperationalFailureCodesAreStableAndSafe; additional cases/helpers |
| `management/control.go` | 48 | implementation contracts and private invariants |
| `management/control_test.go` | 0 | TestCommandValidateRequiresBoundedMutationEnvelope; TestCommandValidateSupportsExplicitAdministrativeTargets; additional cases/helpers |
| `management/desired.go` | 15 | implementation contracts and private invariants |
| `management/desired_example_test.go` | 0 | ExampleDesiredStateReconciler_Reconcile |
| `management/desired_test.go` | 0 | TestDesiredRecordValidatesRevisionedLifecycleState; TestDesiredStateReconcilerAppliesMonotonicTargetScopedRevisions; additional cases/helpers |
| `management/errors.go` | 0 | implementation contracts and private invariants |
| `management/errors_test.go` | 0 | TestManagementErrorsDistinguishStableOperationOutcomes |
| `management/lifecycle.go` | 10 | implementation contracts and private invariants |
| `management/lifecycle_test.go` | 0 | TestWorkerLifecycleBalancesOneAdmission; TestWorkerLifecycleBlockedTransitionsHonorCanceledContext; additional cases/helpers |
| `management/mutation_boundaries_test.go` | 0 | TestManagementExactBoundariesRemainAccepted; TestQueueStatusMeasurementSupportControlsValidation; additional cases/helpers |
| `management/protocol.go` | 28 | implementation contracts and private invariants |
| `management/protocol_test.go` | 0 | TestNegotiateClassifiesVersionsAndIntersectsCapabilities; TestNegotiateRejectsInvalidSupportedRange |
| `management/reader.go` | 5 | implementation contracts and private invariants |
| `management/reader_test.go` | 0 | TestStatusReaderListsBoundedValidatedProviderPages; TestStatusReaderFailsClosedAtConstructionAndProviderBoundaries |
| `management/records.go` | 62 | implementation contracts and private invariants |
| `management/records_test.go` | 0 | TestJobRecordValidateRequiresBoundedRedactedMetadata; TestJobRecordValidateControlsPrivilegedPayloadDisclosure; additional cases/helpers |
| `management/status.go` | 53 | implementation contracts and private invariants |
| `management/status_test.go` | 0 | TestWorkerStatusValidateRejectsMalformedOrUnboundedReports; TestStatusMetadataValidatesNativeReporterIdentity; additional cases/helpers |
| `management_lifecycle.go` | 0 | implementation contracts and private invariants |
| `management_lifecycle_test.go` | 0 | TestQueueManagementLifecyclePausesAndResumesAdmission; TestQueueManagementLifecyclePausesAnEmptyAdmission; additional cases/helpers |
| `managementhttp/control.go` | 0 | implementation contracts and private invariants |
| `managementhttp/control_fuzz_test.go` | 0 | FuzzHandlerCommandBody |
| `managementhttp/control_test.go` | 0 | TestClientAcceptsValidBoundedCommandRequest; TestCommandHandlerRejectsEachUnsafeBoundaryImmediately; additional cases/helpers |
| `managementhttp/example_test.go` | 0 | ExampleClient_Execute; ExampleClient_ListFailures |
| `managementhttp/fleet.go` | 13 | implementation contracts and private invariants |
| `managementhttp/fleet_test.go` | 0 | TestFleetClientForwardsSharedRecordsAndReportsPartialFanout; TestFleetClientAggregatesStatusAndFansOutQueueLifecycle; additional cases/helpers |
| `managementhttp/records.go` | 0 | implementation contracts and private invariants |
| `managementhttp/records_test.go` | 0 | TestRecordClientRejectsEachUnsafeResponseBoundaryImmediately; TestRecordQueryAndDiagnosticsConditionsAreIndependent; additional cases/helpers |
| `managementhttp/status.go` | 13 | implementation contracts and private invariants |
| `managementhttp/status_fuzz_test.go` | 0 | FuzzStatusHandlerFailsClosed |
| `managementhttp/status_test.go` | 0 | TestClientReadsAuthenticatedBoundedStatus; TestClientAndFleetRejectRedirectsWithoutMutatingCallerClient; additional cases/helpers |
| `metric.go` | 0 | implementation contracts and private invariants |
| `metric_test.go` | 0 | TestQueueUsesConfiguredMetric; TestMetricData |
| `mocks/mock_queued_message.go` | 1 | external MockGen output; inspect producer, do not manually edit |
| `mocks/mock_task_message.go` | 1 | external MockGen output; inspect producer, do not manually edit |
| `mocks/mock_worker.go` | 1 | external MockGen output; inspect producer, do not manually edit |
| `mocks/mocks.go` | 1 | implementation contracts and private invariants |
| `nats/client_compatibility_test.go` | 0 | TestWorkerReconnectPreservesPayload; TestShutdownRepublishesPendingPayload |
| `nats/constructor_test.go` | 0 | TestNewWorkerEReturnsConnectionError |
| `nats/decoder_fuzz_test.go` | 0 | FuzzRequestDelivery |
| `nats/nats.go` | 1 | implementation contracts and private invariants |
| `nats/nats_test.go` | 0 | TestMain; TestDefaultFlow; additional cases/helpers |
| `nats/options.go` | 0 | implementation contracts and private invariants |
| `nats/worker_test.go` | 0 | TestOptionsConfigureNATS; TestWorkerQueuesRequestsRunsAndShutsDown; additional cases/helpers |
| `nsq/constructor_test.go` | 0 | TestNewWorkerERequiresAddress |
| `nsq/deadletter.go` | 1 | implementation contracts and private invariants |
| `nsq/decoder_fuzz_test.go` | 0 | FuzzRequestDelivery; FuzzDeadLetterRecord |
| `nsq/nsq.go` | 0 | implementation contracts and private invariants |
| `nsq/nsq_test.go` | 0 | TestMain; TestNSQDefaultFlow; additional cases/helpers |
| `nsq/options.go` | 3 | implementation contracts and private invariants |
| `nsq/settlement_test.go` | 0 | TestRequestDefersNSQSettlement; TestRequestFinishesMalformedNSQMessages; additional cases/helpers |
| `nsq/worker_test.go` | 0 | TestOptionsConfigureNSQ; TestWorkerConstructorAndShutdown; additional cases/helpers |
| `observer.go` | 24 | implementation contracts and private invariants |
| `observer_test.go` | 0 | TestObserverReceivesSuccessfulTaskLifecycle; TestObserverReceivesRetryAndFailure |
| `options.go` | 0 | implementation contracts and private invariants |
| `options_test.go` | 0 | TestWithRetryInterval; TestNewOptionsUsesBoundedAndNilSafeDefaults; additional cases/helpers |
| `pool.go` | 0 | implementation contracts and private invariants |
| `pool_test.go` | 0 | TestNewPoolWithQueueTask; TestPoolNumber |
| `queue.go` | 0 | implementation contracts and private invariants |
| `queue_example_test.go` | 0 | ExampleWithWorkerLifecycle; ExampleQueue_CloseAdmission; additional cases/helpers |
| `queue_test.go` | 0 | TestMain; TestNewQueueWithZeroWorker; additional cases/helpers |
| `queueservice/example_test.go` | 0 | ExampleNewWorker; ExampleNewLifecycleWorker |
| `queueservice/facade.go` | 1 | implementation contracts and private invariants |
| `queueservice/facade_benchmark_test.go` | 0 | BenchmarkLegacyProducerPublish |
| `queueservice/facade_coverage_test.go` | 0 | TestLegacyErrorMethodsPreserveReleasedClassification; TestLegacyCallbackErrorsPreserveEveryOperation; additional cases/helpers |
| `queueservice/facade_fuzz_test.go` | 0 | FuzzLegacyStructuredErrorTranslation |
| `queueservice/facade_internal_test.go` | 0 | TestCompatibilityErrorPreservesUnknownWrapper; TestOptionsErrorFormatsTheCurrentLegacySentinel; additional cases/helpers |
| `queueservice/facade_test.go` | 0 | TestLegacySentinelsPreserveSuccessorIdentity; TestLegacyConstructorsPreserveSuccessorValidation; additional cases/helpers |
| `rabbitmq/example_test.go` | 0 | ExampleNewWorkerE; ExampleNativeConfig |
| `rabbitmq/facade.go` | 25 | implementation contracts and private invariants |
| `rabbitmq/facade_benchmark_test.go` | 0 | BenchmarkLegacyWorkerQueue |
| `rabbitmq/facade_fuzz_test.go` | 0 | FuzzLegacyOptionValuesReachSuccessor |
| `rabbitmq/facade_integration_test.go` | 0 | TestLegacyPublicConstructorOpensPublishedSuccessor |
| `rabbitmq/facade_internal_test.go` | 0 | TestLegacyWorkerDelegatesEveryRuntimeOperation; TestLegacyWorkerZeroValuePreservesReleasedState; additional cases/helpers |
| `rabbitmq/facade_test.go` | 0 | TestLegacyFacadePreservesContracts; TestLegacyOptionsDelegateToSuccessor; additional cases/helpers |
| `recovery.go` | 0 | implementation contracts and private invariants |
| `redisdb/benchmark_test.go` | 0 | BenchmarkRedisEnqueue; BenchmarkRedisConsume; additional cases/helpers |
| `redisdb/constructor_test.go` | 0 | TestNewWorkerEReturnsInvalidConnectionString |
| `redisdb/decoder_fuzz_test.go` | 0 | FuzzRequestDelivery |
| `redisdb/options.go` | 2 | implementation contracts and private invariants |
| `redisdb/redis.go` | 0 | implementation contracts and private invariants |
| `redisdb/worker_test.go` | 0 | TestOptionsConfigureRedisPubSub; TestDefaultRunFunctionSucceeds; additional cases/helpers |
| `redisdb/zzz_integration_test.go` | 0 | TestMain; TestWithRedis; additional cases/helpers |
| `redisstream/facade.go` | 47 | implementation contracts and private invariants |
| `redisstream/facade_test.go` | 0 | TestLegacyFacadePreservesContracts; TestLegacyOptionsDelegateToSuccessor; additional cases/helpers |
| `ring.go` | 0 | implementation contracts and private invariants |
| `ring_test.go` | 0 | TestMaxCapacity; TestRingCapacityIsAtomicUnderConcurrentEnqueue; additional cases/helpers |
| `settlement_test.go` | 0 | TestSuccessfulHandlerAcknowledgesDelivery; TestFailedHandlerRejectsDelivery; additional cases/helpers |
| `thread.go` | 0 | implementation contracts and private invariants |
| `thread_test.go` | 0 | TestRoutineGroupRun; TestRoutineGroupWaitContextCanBeCanceledWithoutLeakingAWaiter |
| `valkeystream/aaa_mutation_boundaries_test.go` | 0 | TestNativeClientOptionsCloneTLSConfiguration; TestNativeResponseBoundaryParsersRejectMalformedValues; additional cases/helpers |
| `valkeystream/api_compat_test.go` | 0 | TestValkeyStreamPublicAPIDoesNotLeakNativeClients; TestValkeyStreamStatsRemainPackageOwned |
| `valkeystream/benchmark_test.go` | 0 | BenchmarkValkeyStreamEnqueue; BenchmarkValkeyStreamConsume; additional cases/helpers |
| `valkeystream/conformance_test.go` | 0 | TestValkeyStreamWorkerConformance |
| `valkeystream/control.go` | 1 | implementation contracts and private invariants |
| `valkeystream/control_test.go` | 0 | TestNativeControllerRetriesDeletesBulkRetriesAndPurges; TestNativeControllerFailsClosedAndReportsMutationUncertainty; additional cases/helpers |
| `valkeystream/errors.go` | 0 | implementation contracts and private invariants |
| `valkeystream/fuzz_test.go` | 0 | FuzzDeliveryEnvelope; FuzzOptions; additional cases/helpers |
| `valkeystream/main_test.go` | 0 | TestMain |
| `valkeystream/native_fault_test.go` | 0 | TestNativeResponseParsersRejectMalformedData; TestAlternatingFieldsRequireStringPairs; additional cases/helpers |
| `valkeystream/native_transport.go` | 0 | implementation contracts and private invariants |
| `valkeystream/native_transport_test.go` | 0 | TestNativeConnectionBufferBoundIsThirtyTwoKiB; TestNativeClientOptionsAreBoundedAndStandalone; additional cases/helpers |
| `valkeystream/options.go` | 0 | implementation contracts and private invariants |
| `valkeystream/options_test.go` | 0 | TestOptionsUseBoundedStandaloneDefaults; TestOptionsApplyExplicitConfiguration; additional cases/helpers |
| `valkeystream/publisher.go` | 0 | implementation contracts and private invariants |
| `valkeystream/publisher_test.go` | 0 | TestPublisherQueuesWithoutCreatingConsumerGroup; TestPublisherRejectsInvalidMessagesAndHonorsCapacity; additional cases/helpers |
| `valkeystream/records.go` | 0 | implementation contracts and private invariants |
| `valkeystream/records_test.go` | 0 | TestNativeManagementRecordsListAndInspect; TestManagementRecordsPopulateSuppliedJobMetadata; additional cases/helpers |
| `valkeystream/status.go` | 0 | implementation contracts and private invariants |
| `valkeystream/status_test.go` | 0 | TestValkeyStreamNativeManagementStatus; TestValkeyStreamReportsConfiguredRetentionCapabilities; additional cases/helpers |
| `valkeystream/worker.go` | 0 | implementation contracts and private invariants |
| `valkeystream/worker_test.go` | 0 | TestWorkerQueuesRequestsSettlesAndShutsDown; TestWorkerNackLeavesDeliveryPendingForReclaim; additional cases/helpers |
| `valkeystream/zzz_integration_test.go` | 0 | TestValkey9NativeLifecycleAndStats; TestValkey9PublisherAppendsWithoutConsumerGroup; additional cases/helpers |


## Investigated contracts and disposition

All five module minima, dependencies, apidiff baselines, generated bodies,
fixtures, build constraints and released legacy facades remain unchanged.
The 134 assurance files were inventoried by package, entrypoint, assertions,
helpers, integration constraints and resource seams. Existing fuzz seeds exercise
JSON/metadata, transport responses, settlement and legacy forwarding; benchmarks
measure Ring/job admission, stream/producer operations and lifecycle wrappers,
not a basis for new performance guarantees. Examples with output are distinct
from two RabbitMQ connection examples that only compile. Integration variants
require their native broker/container/Kind recipes and are not executed for prose.

| Owner / boundary | Source-backed clarification | Evidence / retained limitation |
| --- | --- | --- |
| Root scheduler / Ring | Default is 10,000 pending references; zero explicitly unbounds. Buffer halves at or below half occupancy. | `NewOptions`, `Ring.Request`, `TestRingShrinksAfterDequeue`; active work is not queue depth; zero Ring is not usable. |
| Job codec / admission | JSON, not msgpack; only first AllowOption applies. Payload aliases caller bytes; metadata maps/time are cloned. | `job.NewMessage`, `NewOptions`, `Payload`, metadata isolation tests. Encoding does not validate/limit and can panic; decoding bounds input before unmarshal. |
| Retry execution | One timeout covers local attempts/delays; RetryCount is consumed. Backoff fields validate only when backoff is selected. | `Queue.handle`, `Message.Validate`; local retry differs from broker attempt count and no exactly-once effects follow. |
| Shutdown / release | ReleaseContext waits tracked wrappers, not a timeout-ignoring inner handler. Worker.Shutdown and synchronous collaborators are not preempted by the caller context. | `handle`, `drainContext`, `Shutdown`, routineGroup and service callbacks. A concrete noncooperative callback remains an application-owned resource risk. |
| Error / observer / metric boundaries | Panics are contained at selected callback boundaries, not a blanket arbitrary-code guarantee. Cause graphs remain inspectable; code/metadata validation is not content redaction. | `NewFailure`, `ResolveFailure`, safeerr and callback wrappers. Resolution visits at most 64 nodes; caller error methods can block/panic. Metric unsigned totals can wrap. |
| Desired reconciliation | Providers run under the reconciliation mutex; configured order and successful local revision marks are retained. | `Reconcile`; reentry deadlocks, lock acquisition is not context-aware, an Applier error after effects can be retried. No durable at-most-once guarantee. |
| Lifecycle commands | Bounded process-local results are never evicted; cancellation leaves requested state applied, not rolled back. Commands retain nested selection/replay pointers. | `transition`, `commandEntry`, existing canceled transition tests. Caller counters must balance; uint32 increments have no overflow guard. |
| Status pagination | Each page re-fetches all providers before sorting; nested worker slices are shallow aliases. | `ProviderStatusReader`; offsets do not name a stable snapshot. Page limits do not bound total provider work. |
| Administrative DTOs / HTTP | Shape validation does not authenticate actor/tenant or redact summaries/tags. Bearer credential grants configured routes including reveal. HTTP transport is shallow-copied; redirect behavior is replaced. | management validators, NewHandler/NewClient; deployment owns destination/TLS, provider concurrency, authorization and server limits. |
| Fleet | Fanout is bounded by endpoint count, not total pages/time. Queue status deduplicates by queue name; record operations use first sorted endpoint only. | `FleetClient`; shared logical queue/record store is a caller assumption. Aggregate outcomes can be partial/unknown; no distributed transaction. |
| Core NATS / Redis Pub/Sub | Transient, without durable settlement/replay. Publish acceptance is not handler completion. | native workers; Redis Queue uses Background and driver limits, NATS is not JetStream. |
| NSQ / RabbitMQ | Terminal/retry publication precedes source FIN/ack; failure after publication can duplicate. RabbitMQ requires manual native policy and stable identity callbacks. | settlement helpers and fault/attempt tests; callback execution is synchronous and queue topology is infrastructure-owned. |
| Redis Streams | Direct capacity is checked in Lua; admin capacity/read/append/register/ack are separate commands. Exact count retention is opt-in. | enqueue script, `redisStreamHasCapacity`, `appendRecordWithLineage`, existing partial outcome tests; cross-client admin races and crash windows remain. |
| Valkey Streams | Resolver accepts one through 100; allowlisted replay is implemented. Reclaim idle time is not exclusive execution fencing; replay registration is count-evicted. | resolver, terminalFailure, ReplayRecord script; native shutdown context bounds loop wait, not synchronous Close. TLS Clone preserves verification flags and nested sharing. |
| Legacy facades | Correct unversioned bindings and error ownership stay intact; successor implementation remains the runtime owner. | facade source and source-compatibility/forwarding tests; deprecated import paths and public types remain frozen. |
| Private tooling / diagnostics | SemVer successor uses machine-int arithmetic with unchecked range/overflow. stack returns bytes after its buffer is returned to the pool and includes source lines. | `NextStable`, `stack`; neither arbitrary precision nor owned/redacted immutable stack bytes is claimed. |

### Generated producer availability and inherited mismatch

The three unchanged MockGen headers/directives and root module path agree.
The headers do not identify the original generator version. The root
`go.uber.org/mock v0.6.0` pin identifies the runtime module, not authenticated
historical producer provenance. No ambient mockgen was present initially.

A native Go1.27.2 build of `go.uber.org/mock/mockgen` from authenticated v0.6.0
(public SumDB, its native dependency graph) succeeds. All three recorded commands
execute in a disposable same-import-path module with the `mocks` working directory.
Outputs generated from original core versus current comment-only core are
byte-identical for every file: this batch does not change generated output.
However, none matches the inherited tracked mocks: v0.6.0 adds private
`isgomock struct{}` markers, and Worker output preserves `task`/`ctx` parameter
names rather than `arg0`/`arg1`. The tracked-output reproduction comparison
therefore fails. This is an inherited producer-version/provenance mismatch,
not a passing regenerated-artifact claim.

Generated bodies, bindings, runtime pins and directives remain untouched. The
three structural manual-review findings remain visible with verified producer
availability and this concrete output disposition. Original producer identity
or separately scoped native regeneration needs maintainer resolution; neither
manual edits nor a silent producer/dependency upgrade belongs in this prose pass.

### Open source questions, not missing prose

Runtime remediation is outside this batch. Follow-up owners should decide whether
to join timeout-ignoring handlers, avoid caller callbacks under reconciliation
locks, make stack snapshots owned before pool return, guard machine-int version
arithmetic, lifecycle counter overflow and MaxInt64 response sentinel addition, and define stronger cross-client
administrative capacity/deduplication windows. Current comments state observable
boundaries without weakening the unchanged Go safety policy or dated model.
No new assumption of tenant authorization, universal diagnostic redaction,
verified TLS configuration, execution fencing or durable command deduplication
is introduced. Source-local bounds do not qualify current live broker behavior.


### Generated follow-up disposition

Repository maintainers own recovery of the historical MockGen version or a
separately reviewed regeneration decision. Completion is a version-qualified
producer/cwd/input recipe reproducing tracked outputs, or an explicitly scoped
native regeneration with affected compatibility review. History shows extraction
commit `b1f8aa0dfd662c7497abfbadd53dda077d74bdf2` only renamed the old module
bindings; both that revision and first standalone history already pinned runtime
mock v0.6.0 without producer-version evidence. No original version is guessed.

CONTRIBUTING, root/nested Makefiles, .golib operations, catalog and CI declare no
mandatory mock-generation byte-match gate. The mismatch belongs to this manual
availability/output investigation; it is not rewritten as a passed gate. The
coordinator accepted unchanged generated outputs and exact source preservation
for this Tier A scope, retaining the concrete provenance follow-up.


## Selected local verification

- Go1.27.2 formatting is clean across all 205 tracked Go sources. Native parser
  proof preserves complete ASTs, every executable token/literal and all 114
  source annotations. One extra parser-optional implicit semicolon appears
  before the closing brace of `queueservice.CallbackPanicError` after expanding
  its inline struct for a field comment; no explicit semicolon or other token
  is normalized away. All 1,588 comment groups render through go/doc/comment.
- `go doc -all .` renders all 25 catalog directories, including four nested
  module roots, with GOWORK=off and unchanged module graphs. All five configured
  baselines are apidiff, so no prose snapshot is regenerated or rewritten.
- `go test . ./management ./managementhttp -run '^Example' -count=1 -timeout=3m -v`
  passes eight actual root/management/HTTP examples. The four child `make docs`
  recipes pass; three include example tests executing five output-bearing
  examples, while adapters/rabbitmq's native recipe renders docs only. The two
  connection examples without output are not claimed as executed cases.
- Authenticated policy-matching public Tools2.3.0 `docs check --module .` passes
  native structure/spelling/links: 112 links, 69 unique, 110 okay, zero errors,
  two exclusions and one redirect. Offline `specification check` passes with
  zero configured decisions/modules. Source checker separately reports zero
  structural errors and the three visible generated reviews adjudicated above.
- Pinned Gitleaks v8.30.1 full changed-file, added-content and staged scopes
  report zero findings. Whole-diff review inspects contract truth, legacy
  forwarding, generated provenance and preservation; no runtime changes occur.
- No full suite, live broker, Docker/Kind, race, fuzz, mutation, benchmark,
  interoperability, release or production request campaign is performed.

These are selected local documentation proofs, not hosted CI or qualification
of live durable delivery. The unchanged generated-artifact reproduction
mismatch is expressly not a passing byte-match claim. All owned native caches,
producer binaries/modules, shadow modules, helpers and renders are disposable;
the coordinator's qualified tooling and scanner are retained.

## Objective Enforcement

The checksum-pinned public Tools v2.4.0 verifier now supplies
`make source-docs-check` as a prerequisite of the existing local and CI Make
routes. Ordinary shared CI selects strict source-documentation checking and
requires its result. Native prerequisites, operation budgets, security and
release controls are retained. Hosted exact-head evidence is recorded by the
coordination ledger; configuration alone does not establish green delivery.

import {
	WorkflowEntrypoint,
	WorkflowStep,
} from "cloudflare:workers";
import type { WorkflowEvent } from "cloudflare:workers";

/**
 * Imvara Project Run Workflow
 *
 * Purpose:
 * Durable orchestration envelope for governed Imvara Project Runs.
 *
 * The Project Run coordinates platform capabilities but does not
 * implement their methodology.
 *
 * Genericity rule:
 * No client-, brand-, Vertical-, Job-, or engagement-specific logic belongs
 * in this Workflow implementation. Engagement context must arrive at runtime.
 *
 * Current Intelligence Processing scope:
 * - Resolve governed Expression-normalization workload.
 * - Execute persisted governed batches through INTELLIGENCE_PROCESSING.
 * - Assign one stable processing_operation_id to each logical batch.
 * - Perform a separate read-only workload verification after each mutation.
 * - Continue until the governed workload reports complete.
 *
 * INTELLIGENCE owns:
 * - normalization methodology
 * - workload eligibility
 * - deterministic batch membership
 * - Expression persistence
 * - Utterance normalization lifecycle
 * - processing-operation idempotency truth
 *
 * INTELLIGENCE_PROCESSING owns:
 * - governed processing transport
 * - consumption of Intelligence workload contracts
 *
 * PROJECT_RUN owns:
 * - durable sequencing
 * - stable logical batch operation identity
 * - retry-safe orchestration
 * - progress accumulation
 */

export type ProjectRunWorkflowParams = {
	engagement_id?: string;
	vertical_version_id?: string;
	project_run_id?: string;
	requested_by?: string;
	started_at?: string;
};

export type GovernanceDecisionPayload = {
	approved?: boolean;
	decision?: string;
	comment?: string;
	decided_by?: string;
	decided_at?: string;
};

type ProcessingError = {
	code?: string;
	message?: string;
};

type IntelligenceProcessingWorkloadResponse = {
	ok?: boolean;

	service?: {
		name?: string;
		service?: string;
		version?: string;
	};

	operation?: {
		type?: string;
		mutation?: boolean;
	};

	vertical_version_id?: string;
	requested_by?: string;

	workload?: {
		status?: "available" | "complete" | string;
		ingestion_run_id?: string | null;
		eligible_utterance_count?: number;
		eligible_utterance_count_for_ingestion_run?: number;
		selected_utterance_count?: number;
		maximum_batch_size?: number;
		remaining_eligible_after_selected_batch?: number;
		utterance_ids?: string[];
	};

	integrity?: {
		not_normalized_with_existing_expression_count?: number;
		conflicts_excluded_from_workload?: boolean;
	};

	intelligence?: {
		status?: number;
		intelligence_version?: string | null;
		vertical_version_status?: string | null;
	};

	error?: ProcessingError;
};

type IntelligenceProcessingBatchPersistResponse = {
	ok?: boolean;

	service?: {
		name?: string;
		service?: string;
		version?: string;
	};

	operation?: {
		type?: string;
		processing_operation_id?: string;
		persistence_requested?: boolean;
		mutation_performed?: boolean;
		replayed?: boolean;
	};

	request_scope?: {
		vertical_version_id?: string;
		created_by?: string;
		project_run_id?: string | null;
		engagement_id?: string | null;
	};

	selected_workload?: {
		ingestion_run_id?: string | null;
		eligible_utterance_count_before_batch?: number;
		selected_utterance_count?: number;
		maximum_batch_size?: number;
		remaining_eligible_after_selected_batch_if_successful?: number;
		utterance_ids?: string[];
	} | null;

	replay_context?: {
		current_workload_status?: string;
		current_ingestion_run_id?: string | null;
		current_selected_utterance_count?: number;
		current_eligible_utterance_count?: number;
		note?: string;
	} | null;

	intelligence?: {
		status?: number;

		response?: {
			ok?: boolean;
			intelligence_version?: string;
			mode?: string;
			persisted?: boolean;
			expressions_created?: number;
			utterances_processed?: number;

			utterance_status_updates?: {
				normalized?: number;
				review_required?: number;
				unparsed_routed_to_review_required?: number;
			};

			idempotency?: {
				enabled?: boolean;
				processing_operation_id?: string;
				status?: string;
				replayed?: boolean;
				resumed_started_operation?: boolean;
				mutation_performed_on_this_request?: boolean;
			};
		};
	};

	mutation_report?: {
		upstream_reported_persisted?: boolean;
		upstream_reported_utterances_processed?: number;
		upstream_reported_expressions_created?: number;

		upstream_reported_status_updates?: {
			normalized?: number;
			review_required?: number;
			unparsed_routed_to_review_required?: number;
		} | null;

		mutation_performed_on_this_request?: boolean;
		replayed_completed_operation?: boolean;
	};

	idempotency?: {
		required?: boolean;
		owner?: string;
		processing_operation_id?: string;
		status?: string;
		replayed?: boolean;
		mutation_performed_on_this_request?: boolean;
		completed_result_authority?: string;
	};

	error?: ProcessingError;
};

type BatchSummary = {
	batch_sequence: number;
	processing_operation_id: string;

	eligible_before_batch: number;
	selected_utterance_count: number;

	utterances_processed: number;
	expressions_created: number;

	normalized: number;
	review_required: number;
	unparsed_routed_to_review_required: number;

	replayed: boolean;
	mutation_performed: boolean;

	eligible_after_read_back: number;
	workload_status_after_read_back: string;
};

export class ProjectRunWorkflow extends WorkflowEntrypoint<
	Env,
	ProjectRunWorkflowParams
> {
	async run(
		event: WorkflowEvent<ProjectRunWorkflowParams>,
		step: WorkflowStep,
	) {
		const instanceId = event.instanceId;

		const notifyStep = async (
			stepName: string,
			status:
				| "running"
				| "completed"
				| "waiting"
				| "error",
		) => {
			try {
				const doId =
					this.env.WORKFLOW_STATUS.idFromName(
						instanceId,
					);

				const stub =
					this.env.WORKFLOW_STATUS.get(
						doId,
					);

				await stub.updateStep(
					stepName,
					status,
				);
			} catch {
				/**
				 * Real-time status delivery is observational.
				 *
				 * A Project Run must not fail merely because the Durable Object
				 * status channel is temporarily unavailable.
				 */
			}
		};

		const readWorkload = async (
			verticalVersionId: string,
			requestedBy: string,
		) => {
			const response =
				await this.env.INTELLIGENCE_PROCESSING.fetch(
					new Request(
						"https://intelligence-processing.internal/expressions/normalize/workload",
						{
							method: "POST",

							headers: {
								"content-type":
									"application/json",
							},

							body: JSON.stringify({
								vertical_version_id:
									verticalVersionId,

								requested_by:
									requestedBy,
							}),
						},
					),
				);

			let body:
				| IntelligenceProcessingWorkloadResponse
				| null = null;

			try {
				body =
					(await response.json()) as IntelligenceProcessingWorkloadResponse;
			} catch {
				throw new Error(
					"INTELLIGENCE_PROCESSING returned a non-JSON workload response.",
				);
			}

			if (
				!response.ok ||
				!body?.ok
			) {
				throw new Error(
					`Intelligence workload resolution failed: ${
						body?.error?.code ??
						"UNKNOWN_ERROR"
					}${
						body?.error?.message
							? ` - ${body.error.message}`
							: ""
					}`,
				);
			}

			const workload =
				body.workload;

			if (
				!workload ||
				![
					"available",
					"complete",
				].includes(
					String(
						workload.status,
					),
				)
			) {
				throw new Error(
					"INTELLIGENCE_PROCESSING returned an invalid workload status.",
				);
			}

			return body;
		};

		await notifyStep(
			"initialize project run",
			"running",
		);

		const initialization = await step.do(
			"initialize project run",
			async () => {
				return {
					instance_id:
						instanceId,

					params: {
						engagement_id:
							event.payload.engagement_id,

						vertical_version_id:
							event.payload.vertical_version_id,

						project_run_id:
							event.payload.project_run_id,

						requested_by:
							event.payload.requested_by,

						started_at:
							event.payload.started_at,
					},

					initialized_at:
						new Date().toISOString(),
				};
			},
		);

		await notifyStep(
			"initialize project run",
			"completed",
		);

		await notifyStep(
			"durability checkpoint",
			"running",
		);

		await step.sleep(
			"durability checkpoint",
			"2 seconds",
		);

		await notifyStep(
			"durability checkpoint",
			"completed",
		);

		await notifyStep(
			"governance checkpoint",
			"waiting",
		);

		const governanceEvent =
			await step.waitForEvent<GovernanceDecisionPayload>(
				"governance checkpoint",
				{
					type:
						"governance-decision",

					timeout:
						"60 minutes",
				},
			);

		await notifyStep(
			"governance checkpoint",
			"completed",
		);

		const verticalVersionId =
			event.payload.vertical_version_id;

		const requestedBy =
			event.payload.requested_by;

		const projectRunId =
			event.payload.project_run_id ??
			null;

		const engagementId =
			event.payload.engagement_id ??
			null;

		if (
			!verticalVersionId ||
			!requestedBy
		) {
			throw new Error(
				"Project Run is missing vertical_version_id or requested_by required for Intelligence Processing.",
			);
		}

		await notifyStep(
			"intelligence processing",
			"running",
		);

		const initialWorkload =
			await step.do(
				"resolve initial intelligence workload",
				async () => {
					return readWorkload(
						verticalVersionId,
						requestedBy,
					);
				},
			);

		let currentWorkload =
			initialWorkload;

		const initialEligibleCount =
			Number(
				currentWorkload.workload
					?.eligible_utterance_count ??
					0,
			);

		const batchSummaries:
			BatchSummary[] = [];

		let batchSequence = 1;

		let totalUtterancesProcessed =
			0;

		let totalExpressionsCreated =
			0;

		let totalNormalized =
			0;

		let totalReviewRequired =
			0;

		let totalUnparsed =
			0;

		while (
			currentWorkload.workload
				?.status ===
			"available"
		) {
			const workloadBefore =
				currentWorkload.workload;

			const eligibleBeforeBatch =
				Number(
					workloadBefore
						.eligible_utterance_count ??
						0,
				);

			const selectedBeforeBatch =
				Number(
					workloadBefore
						.selected_utterance_count ??
						0,
				);

			if (
				selectedBeforeBatch <=
				0
			) {
				throw new Error(
					"Available Intelligence workload contained no selected Utterances.",
				);
			}

			/**
			 * Stable logical operation identity.
			 *
			 * event.instanceId is immutable for this Workflow instance.
			 * batchSequence is deterministic because each next batch is
			 * entered only after the previous mutation has been independently
			 * read back.
			 *
			 * INTELLIGENCE persists this identity and replays its completed
			 * result if Cloudflare retries the same logical Workflow step.
			 */
			const processingOperationId =
				`${instanceId}:expression-normalization:batch:${String(
					batchSequence,
				).padStart(
					6,
					"0",
				)}`;

			const batchResult =
				await step.do(
					`persist intelligence batch ${batchSequence}`,
					async () => {
						const response =
							await this.env.INTELLIGENCE_PROCESSING.fetch(
								new Request(
									"https://intelligence-processing.internal/expressions/normalize/batch-persist",
									{
										method:
											"POST",

										headers: {
											"content-type":
												"application/json",
										},

										body:
											JSON.stringify(
												{
													vertical_version_id:
														verticalVersionId,

													created_by:
														requestedBy,

													processing_operation_id:
														processingOperationId,

													project_run_id:
														projectRunId,

													engagement_id:
														engagementId,
												},
											),
									},
								),
							);

						let body:
							| IntelligenceProcessingBatchPersistResponse
							| null = null;

						try {
							body =
								(await response.json()) as IntelligenceProcessingBatchPersistResponse;
						} catch {
							throw new Error(
								"INTELLIGENCE_PROCESSING returned a non-JSON batch persistence response.",
							);
						}

						if (
							!response.ok ||
							!body?.ok
						) {
							throw new Error(
								`Intelligence batch persistence failed: ${
									body?.error?.code ??
									"UNKNOWN_ERROR"
								}${
									body?.error?.message
										? ` - ${body.error.message}`
										: ""
								}`,
							);
						}

						if (
							body.idempotency
								?.processing_operation_id !==
								processingOperationId ||
							body.idempotency
								?.status !==
								"completed"
						) {
							throw new Error(
								"INTELLIGENCE_PROCESSING returned a batch result without the expected completed processing operation identity.",
							);
						}

						if (
							body.intelligence
								?.response
								?.persisted !==
							true
						) {
							throw new Error(
								"INTELLIGENCE_PROCESSING returned a completed operation that did not report persisted Intelligence truth.",
							);
						}

						return body;
					},
				);

			/**
			 * Separate read-only verification.
			 *
			 * The batch mutation response itself is never treated as proof that
			 * the governed workload advanced.
			 */
			const workloadAfter =
				await step.do(
					`verify intelligence batch ${batchSequence}`,
					async () => {
						return readWorkload(
							verticalVersionId,
							requestedBy,
						);
					},
				);

			const eligibleAfterBatch =
				Number(
					workloadAfter
						.workload
						?.eligible_utterance_count ??
						0,
				);

			/**
			 * A successful first mutation should remove at least the selected
			 * governed batch from eligibility.
			 *
			 * "At least" is intentional: this assertion does not incorrectly
			 * fail if another governed actor also advances eligible work.
			 */
			const minimumExpectedReduction =
				selectedBeforeBatch;

			const actualReduction =
				eligibleBeforeBatch -
				eligibleAfterBatch;

			if (
				actualReduction <
				minimumExpectedReduction
			) {
				throw new Error(
					`Intelligence batch read-back did not prove workload progression. Expected reduction of at least ${minimumExpectedReduction}; observed ${actualReduction}.`,
				);
			}

			const mutationReport =
				batchResult.mutation_report;

			const statusUpdates =
				mutationReport
					?.upstream_reported_status_updates;

			const utterancesProcessed =
				Number(
					mutationReport
						?.upstream_reported_utterances_processed ??
						0,
				);

			const expressionsCreated =
				Number(
					mutationReport
						?.upstream_reported_expressions_created ??
						0,
				);

			const normalized =
				Number(
					statusUpdates
						?.normalized ??
						0,
				);

			const reviewRequired =
				Number(
					statusUpdates
						?.review_required ??
						0,
				);

			const unparsed =
				Number(
					statusUpdates
						?.unparsed_routed_to_review_required ??
						0,
				);

			totalUtterancesProcessed +=
				utterancesProcessed;

			totalExpressionsCreated +=
				expressionsCreated;

			totalNormalized +=
				normalized;

			totalReviewRequired +=
				reviewRequired;

			totalUnparsed +=
				unparsed;

			batchSummaries.push({
				batch_sequence:
					batchSequence,

				processing_operation_id:
					processingOperationId,

				eligible_before_batch:
					eligibleBeforeBatch,

				selected_utterance_count:
					selectedBeforeBatch,

				utterances_processed:
					utterancesProcessed,

				expressions_created:
					expressionsCreated,

				normalized,

				review_required:
					reviewRequired,

				unparsed_routed_to_review_required:
					unparsed,

				replayed:
					batchResult.idempotency
						?.replayed ===
					true,

				mutation_performed:
					batchResult.idempotency
						?.mutation_performed_on_this_request ===
					true,

				eligible_after_read_back:
					eligibleAfterBatch,

				workload_status_after_read_back:
					String(
						workloadAfter
							.workload
							?.status ??
							"unknown",
					),
			});

			currentWorkload =
				workloadAfter;

			batchSequence += 1;
		}

		if (
			currentWorkload.workload
				?.status !==
			"complete"
		) {
			throw new Error(
				"Intelligence Processing ended without a governed complete workload state.",
			);
		}

		await notifyStep(
			"intelligence processing",
			"completed",
		);

		const intelligenceProcessing = {
			status:
				"complete",

			persistence_performed:
				batchSummaries.length >
				0,

			verification:
				"Each persisted batch was followed by a separate read-only governed workload read-back.",

			initial_eligible_utterance_count:
				initialEligibleCount,

			final_eligible_utterance_count:
				Number(
					currentWorkload
						.workload
						?.eligible_utterance_count ??
						0,
				),

			batches_completed:
				batchSummaries.length,

			utterances_processed:
				totalUtterancesProcessed,

			expressions_created:
				totalExpressionsCreated,

			utterance_status_updates: {
				normalized:
					totalNormalized,

				review_required:
					totalReviewRequired,

				unparsed_routed_to_review_required:
					totalUnparsed,
			},

			integrity: {
				not_normalized_with_existing_expression_count:
					Number(
						currentWorkload
							.integrity
							?.not_normalized_with_existing_expression_count ??
							0,
					),

				conflicts_excluded_from_workload:
					currentWorkload
						.integrity
						?.conflicts_excluded_from_workload ===
					true,
			},

			batches:
				batchSummaries,

			discipline: {
				durable_iteration:
					true,

				stable_operation_identity:
					true,

				idempotency_authority:
					"INTELLIGENCE",

				workload_authority:
					"INTELLIGENCE",

				batch_membership_authority:
					"INTELLIGENCE",

				methodology_authority:
					"INTELLIGENCE",

				project_run_role:
					"Durable sequencing and verification only.",

				caller_selected_utterances:
					false,
			},
		};

		await notifyStep(
			"complete project run envelope",
			"running",
		);

		const completion = await step.do(
			"complete project run envelope",
			async () => {
				return {
					instance_id:
						instanceId,

					initialization,

					governance_decision: {
						approved:
							governanceEvent
								.payload
								.approved,

						decision:
							governanceEvent
								.payload
								.decision,

						comment:
							governanceEvent
								.payload
								.comment,

						decided_by:
							governanceEvent
								.payload
								.decided_by,

						decided_at:
							governanceEvent
								.payload
								.decided_at,
					},

					intelligence_processing:
						intelligenceProcessing,

					completed_at:
						new Date().toISOString(),
				};
			},
		);

		await notifyStep(
			"complete project run envelope",
			"completed",
		);

		return completion;
	}
}

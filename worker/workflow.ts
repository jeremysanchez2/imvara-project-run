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
 */

export type ProjectRunWorkflowParams = {
	engagement_id?: string;
	vertical_version_id?: string;
	project_run_id?: string;
	requested_by?: string;
	started_at?: string;
};

/**
 * Generic governance event payload.
 *
 * Cloudflare Workflow step/event values must be serializable.
 *
 * This contract deliberately contains only generic governance metadata.
 * The meaning and consequences of a decision belong to governed runtime data
 * and platform contracts, not this Workflow.
 */
export type GovernanceDecisionPayload = {
	approved?: boolean;
	decision?: string;
	comment?: string;
	decided_by?: string;
	decided_at?: string;
};

type IntelligenceProcessingWorkloadResponse = {
	ok?: boolean;

	workload?: {
		status?: string;
		ingestion_run_id?: string | null;
		selected_utterance_count?: number;
		utterance_ids?: string[];
		maximum_batch_size?: number;
	};

	read_window?: {
		returned_utterance_count?: number;
		reported_utterance_count?: number;
		eligible_utterance_count?: number;
		ingestion_run_count?: number;
		full_vertical_workload_known?: boolean;
	};

	error?: {
		code?: string;
		message?: string;
	};
};

type IntelligenceNormalizationPreviewResponse = {
	ok?: boolean;

	operation?: {
		type?: string;
		persistence_requested?: boolean;
	};

	intelligence?: {
		status?: number;

		response?: {
			ok?: boolean;
			intelligence_version?: string;
			mode?: string;
			persisted?: boolean;

			proposals?: Array<{
				utterance_id?: string;
				parsing_status?: string;
				expressions?: Array<{
					expression_index?: number;
					normalized_text?: string;
				}>;
			}>;

			failures?: Array<{
				utterance_id?: string;
				error?: string;
				errors?: string[];
			}>;
		};
	};

	error?: {
		code?: string;
		message?: string;
	};
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

		/**
		 * Intelligence Processing preview
		 *
		 * v1 scope:
		 * - Resolve the next governed Expression-normalization workload.
		 * - Invoke normalization in preview mode only.
		 * - Perform no Expression persistence.
		 *
		 * The Project Run does not select model, prompt, method version,
		 * batch size, Utterances, or ingestion run.
		 *
		 * INTELLIGENCE_PROCESSING resolves those operational inputs through
		 * governed platform contracts.
		 */
		await notifyStep(
			"intelligence processing preview",
			"running",
		);

		const intelligenceProcessing =
			await step.do(
				"intelligence processing preview",
				async () => {
					const verticalVersionId =
						event.payload
							.vertical_version_id;

					const requestedBy =
						event.payload.requested_by;

					if (
						!verticalVersionId ||
						!requestedBy
					) {
						throw new Error(
							"Project Run is missing vertical_version_id or requested_by required for Intelligence Processing.",
						);
					}

					const workloadResponse =
						await this.env.INTELLIGENCE_PROCESSING.fetch(
							new Request(
								"https://intelligence-processing.internal/expressions/normalize/workload",
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

												requested_by:
													requestedBy,
											},
										),
								},
							),
						);

					let workload:
						| IntelligenceProcessingWorkloadResponse
						| null = null;

					try {
						workload =
							(await workloadResponse.json()) as IntelligenceProcessingWorkloadResponse;
					} catch {
						throw new Error(
							"INTELLIGENCE_PROCESSING returned a non-JSON workload response.",
						);
					}

					if (
						!workloadResponse.ok ||
						!workload?.ok
					) {
						throw new Error(
							`Intelligence workload resolution failed: ${
								workload?.error?.code ??
								"UNKNOWN_ERROR"
							}${
								workload?.error?.message
									? ` - ${workload.error.message}`
									: ""
							}`,
						);
					}

					if (
						workload.workload
							?.status !==
						"available"
					) {
						return {
							status:
								"no_eligible_workload",

							workload,

							normalization_preview:
								null,

							persistence_performed:
								false,
						};
					}

					const ingestionRunId =
						workload.workload
							.ingestion_run_id;

					const utteranceIds =
						Array.isArray(
							workload.workload
								.utterance_ids,
						)
							? workload.workload
									.utterance_ids
							: [];

					if (
						!ingestionRunId ||
						utteranceIds.length ===
							0
					) {
						throw new Error(
							"INTELLIGENCE_PROCESSING returned available workload without ingestion_run_id and utterance_ids.",
						);
					}

					const previewResponse =
						await this.env.INTELLIGENCE_PROCESSING.fetch(
							new Request(
								"https://intelligence-processing.internal/expressions/normalize/preview",
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

												ingestion_run_id:
													ingestionRunId,

												created_by:
													requestedBy,

												utterance_ids:
													utteranceIds,
											},
										),
								},
							),
						);

					let preview:
						| IntelligenceNormalizationPreviewResponse
						| null = null;

					try {
						preview =
							(await previewResponse.json()) as IntelligenceNormalizationPreviewResponse;
					} catch {
						throw new Error(
							"INTELLIGENCE_PROCESSING returned a non-JSON normalization preview response.",
						);
					}

					if (
						!previewResponse.ok ||
						!preview?.ok
					) {
						throw new Error(
							`Intelligence normalization preview failed: ${
								preview?.error?.code ??
								"UNKNOWN_ERROR"
							}${
								preview?.error?.message
									? ` - ${preview.error.message}`
									: ""
							}`,
						);
					}

					return {
						status:
							"preview_complete",

						workload,

						normalization_preview:
							preview,

						persistence_performed:
							false,
					};
				},
			);

		await notifyStep(
			"intelligence processing preview",
			"completed",
		);

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

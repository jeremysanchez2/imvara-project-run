import {
	WorkflowEntrypoint,
	WorkflowStep,
} from "cloudflare:workers";
import type { WorkflowEvent } from "cloudflare:workers";

/**
 * Imvara Project Run Workflow
 *
 * Purpose:
 * Establish and verify the durable Cloudflare Workflow execution envelope
 * before connecting Project Runs to Imvara Runtime and MOM capabilities.
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
 * Do not use Record<string, unknown> here because `unknown` does not satisfy
 * Cloudflare's Workflow Serializable constraint.
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
			status: "running" | "completed" | "waiting" | "error",
		) => {
			try {
				const doId =
					this.env.WORKFLOW_STATUS.idFromName(instanceId);

				const stub =
					this.env.WORKFLOW_STATUS.get(doId);

				await stub.updateStep(stepName, status);
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
					instance_id: instanceId,
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
					initialized_at: new Date().toISOString(),
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
					type: "governance-decision",
					timeout: "60 minutes",
				},
			);

		await notifyStep(
			"governance checkpoint",
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
					instance_id: instanceId,
					initialization,
					governance_decision: {
						approved:
							governanceEvent.payload.approved,
						decision:
							governanceEvent.payload.decision,
						comment:
							governanceEvent.payload.comment,
						decided_by:
							governanceEvent.payload.decided_by,
						decided_at:
							governanceEvent.payload.decided_at,
					},
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

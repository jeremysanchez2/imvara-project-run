import {
	env,
	introspectWorkflowInstance,
} from "cloudflare:test";
import {
	describe,
	it,
	expect,
} from "vitest";

describe("ProjectRunWorkflow", () => {
	it(
		"completes the durable Project Run envelope after governance decision",
		async () => {
			const instanceId =
				`project-run-test-${Date.now()}`;

			await using instance =
				await introspectWorkflowInstance(
					env.PROJECT_RUN_WORKFLOW,
					instanceId,
				);

			await instance.modify(async (m) => {
				await m.disableSleeps();

				await m.mockEvent({
					type: "governance-decision",
					payload: {
						approved: true,
						decision: "approved",
						comment:
							"Workflow test governance decision",
					},
				});
			});

			await env.PROJECT_RUN_WORKFLOW.create({
				id: instanceId,
				params: {
					engagement_id:
						"test-engagement",
					vertical_version_id:
						"test-vertical:v1",
					project_run_id:
						instanceId,
					requested_by:
						"workflow-test",
					started_at:
						new Date().toISOString(),
				},
			});

			const result =
				await instance.waitForStepResult({
					name:
						"complete project run envelope",
				});

			expect(result).toMatchObject({
				instance_id: instanceId,
				governance_decision: {
					approved: true,
					decision: "approved",
					comment:
						"Workflow test governance decision",
				},
			});

			expect(result).toHaveProperty(
				"initialization",
			);

			expect(result).toHaveProperty(
				"completed_at",
			);
		},
	);

	it(
		"errors when the governance checkpoint times out",
		async () => {
			const instanceId =
				`project-run-timeout-test-${Date.now()}`;

			await using instance =
				await introspectWorkflowInstance(
					env.PROJECT_RUN_WORKFLOW,
					instanceId,
				);

			await instance.modify(async (m) => {
				await m.disableSleeps();

				await m.forceEventTimeout({
					name:
						"governance checkpoint",
				});
			});

			await env.PROJECT_RUN_WORKFLOW.create({
				id: instanceId,
				params: {
					project_run_id:
						instanceId,
				},
			});

			await expect(
				instance.waitForStatus("errored"),
			).resolves.not.toThrow();
		},
	);
});

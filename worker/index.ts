// Export the governed Project Run Workflow and existing Durable Object class.
export { ProjectRunWorkflow } from "./workflow";
export { WorkflowStatusDO } from "./durable-object";

/**
 * Imvara Project Run Worker
 *
 * Purpose:
 * Provide the HTTP/WebSocket control plane for durable Project Run Workflow
 * instances.
 *
 * Current production boundary:
 * - This Worker starts and controls Cloudflare Workflow instances.
 * - Workflow execution is durable.
 * - Workflow status can be observed through the Durable Object/WebSocket.
 * - Engagement context is supplied at runtime.
 *
 * Not yet connected here:
 * - Imvara Runtime Orchestrator
 * - MOM platform capabilities
 * - governed foundation loading
 * - evidence processing
 * - MOM governance decisions
 *
 * Genericity rule:
 * No client-, brand-, Vertical-, Job-, or engagement-specific logic belongs
 * in this Worker. Engagement and foundation identifiers are runtime data.
 */

type ProjectRunStartRequest = {
	engagement_id?: string;
	vertical_version_id?: string;
	project_run_id?: string;
	requested_by?: string;
};

type ProjectRunEventRequest = {
	approved?: boolean;
	comment?: string;
	[key: string]: unknown;
};

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		/**
		 * Health/readiness endpoint.
		 *
		 * This deliberately reports only capabilities established by this Worker.
		 * It does not imply Runtime or MOM execution is connected yet.
		 */
		if (
			url.pathname === "/health" &&
			request.method === "GET"
		) {
			return Response.json({
				ok: true,
				service: "imvara-project-run",
				mode: "durable-project-run-envelope",
				workflow_binding: "PROJECT_RUN_WORKFLOW",
				workflow_class: "ProjectRunWorkflow",
				durable_status: true,
				websocket_status: true,
				runtime_connected: false,
				mom_execution_connected: false,
			});
		}

		/**
		 * Create a durable Project Run Workflow instance.
		 *
		 * All engagement/foundation context is caller supplied. The Worker does
		 * not infer or substitute identifiers.
		 */
		if (
			url.pathname === "/api/workflow/start" &&
			request.method === "POST"
		) {
			try {
				let body: ProjectRunStartRequest = {};

				const contentType =
					request.headers.get("content-type") ?? "";

				if (
					contentType
						.toLowerCase()
						.includes("application/json")
				) {
					body =
						(await request.json()) as ProjectRunStartRequest;
				}

				const instance =
					await env.PROJECT_RUN_WORKFLOW.create({
						params: {
							engagement_id:
								body.engagement_id,
							vertical_version_id:
								body.vertical_version_id,
							project_run_id:
								body.project_run_id,
							requested_by:
								body.requested_by,
							started_at:
								new Date().toISOString(),
						},
					});

				return Response.json(
					{
						ok: true,
						service:
							"imvara-project-run",
						instance_id:
							instance.id,
						workflow_class:
							"ProjectRunWorkflow",
						message:
							"Project Run workflow started successfully",
					},
					{
						status: 201,
					},
				);
			} catch (error) {
				console.error(
					"Failed to start Project Run workflow:",
					error,
				);

				return Response.json(
					{
						ok: false,
						error:
							"Failed to start Project Run workflow",
					},
					{
						status: 500,
					},
				);
			}
		}

		/**
		 * Read the authoritative Cloudflare Workflow instance status.
		 */
		if (
			url.pathname.startsWith(
				"/api/workflow/status/",
			) &&
			request.method === "GET"
		) {
			const instanceId =
				url.pathname.split("/").pop();

			if (!instanceId) {
				return Response.json(
					{
						ok: false,
						error:
							"Instance ID required",
					},
					{
						status: 400,
					},
				);
			}

			try {
				const instance =
					await env.PROJECT_RUN_WORKFLOW.get(
						instanceId,
					);

				const status =
					await instance.status();

				return Response.json({
					ok: true,
					service:
						"imvara-project-run",
					instance_id:
						instanceId,
					status,
				});
			} catch (error) {
				console.error(
					"Failed to get Project Run workflow status:",
					error,
				);

				return Response.json(
					{
						ok: false,
						error:
							"Failed to get Project Run workflow status",
					},
					{
						status: 500,
					},
				);
			}
		}

		/**
		 * Continue a Workflow waiting at its generic governance checkpoint.
		 *
		 * The event payload is retained as caller-supplied runtime context.
		 * This Worker does not infer approval.
		 */
		if (
			url.pathname.startsWith(
				"/api/workflow/event/",
			) &&
			request.method === "POST"
		) {
			const instanceId =
				url.pathname.split("/").pop();

			if (!instanceId) {
				return Response.json(
					{
						ok: false,
						error:
							"Instance ID required",
					},
					{
						status: 400,
					},
				);
			}

			try {
				const body =
					(await request.json()) as ProjectRunEventRequest;

				const instance =
					await env.PROJECT_RUN_WORKFLOW.get(
						instanceId,
					);

				await instance.sendEvent({
					type:
						"user-approval",
					payload: body,
				});

				return Response.json({
					ok: true,
					service:
						"imvara-project-run",
					instance_id:
						instanceId,
					event_type:
						"user-approval",
					message:
						"Project Run workflow event sent successfully",
				});
			} catch (error) {
				console.error(
					"Failed to send Project Run workflow event:",
					error,
				);

				return Response.json(
					{
						ok: false,
						error:
							"Failed to send Project Run workflow event",
					},
					{
						status: 500,
					},
				);
			}
		}

		/**
		 * WebSocket observation channel backed by WorkflowStatusDO.
		 */
		if (
			url.pathname === "/ws" &&
			request.method === "GET"
		) {
			const instanceId =
				url.searchParams.get(
					"instanceId",
				);

			if (!instanceId) {
				return new Response(
					"instanceId query parameter required",
					{
						status: 400,
					},
				);
			}

			const upgradeHeader =
				request.headers.get(
					"Upgrade",
				);

			if (
				upgradeHeader?.toLowerCase() !==
				"websocket"
			) {
				return new Response(
					"Expected Upgrade: websocket",
					{
						status: 426,
					},
				);
			}

			try {
				const doId =
					env.WORKFLOW_STATUS.idFromName(
						instanceId,
					);

				const stub =
					env.WORKFLOW_STATUS.get(
						doId,
					);

				return stub.fetch(
					request,
				);
			} catch (error) {
				console.error(
					"Failed to establish Project Run WebSocket:",
					error,
				);

				return new Response(
					"Failed to establish WebSocket connection",
					{
						status: 500,
					},
				);
			}
		}

		return Response.json(
			{
				ok: false,
				error: "Not Found",
			},
			{
				status: 404,
			},
		);
	},
} satisfies ExportedHandler<Env>;

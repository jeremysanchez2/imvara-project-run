import { DurableObject } from "cloudflare:workers";

/**
 * WorkflowStatusDO
 *
 * Durable status channel for Imvara Project Run Workflow instances.
 *
 * Responsibilities:
 * - Persist Workflow execution status independently of the browser/UI.
 * - Accept hibernating WebSocket connections.
 * - Broadcast Workflow status changes to connected clients.
 * - Provide an idempotent RPC method for Workflow step-status updates.
 *
 * Genericity rule:
 * This Durable Object understands Workflow execution state only.
 * It must not contain client-, brand-, Vertical-, Job-, engagement-,
 * or MOM-method-specific decision logic.
 *
 * The class name remains WorkflowStatusDO because an existing Durable Object
 * migration already establishes this class identity.
 */

type WorkflowStepStatus =
	| "pending"
	| "running"
	| "waiting"
	| "completed"
	| "error";

type WorkflowStatus =
	| "running"
	| "waiting"
	| "completed"
	| "error";

const PROJECT_RUN_ENVELOPE_STEPS = [
	"initialize project run",
	"durability checkpoint",
	"governance checkpoint",
	"complete project run envelope",
] as const;

export class WorkflowStatusDO extends DurableObject {
	private stepStatuses: Map<string, WorkflowStepStatus>;
	private currentStep: string | null;
	private workflowStatus: WorkflowStatus;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		this.stepStatuses = new Map();
		this.currentStep = null;
		this.workflowStatus = "running";

		ctx.blockConcurrencyWhile(async () => {
			const storedStatuses =
				await ctx.storage.get<Record<string, WorkflowStepStatus>>(
					"stepStatuses",
				);

			const storedCurrent =
				await ctx.storage.get<string | null>(
					"currentStep",
				);

			const storedWorkflowStatus =
				await ctx.storage.get<WorkflowStatus>(
					"workflowStatus",
				);

			if (storedStatuses) {
				this.stepStatuses = new Map(
					Object.entries(storedStatuses),
				);
			} else {
				for (const stepName of PROJECT_RUN_ENVELOPE_STEPS) {
					this.stepStatuses.set(
						stepName,
						"pending",
					);
				}
			}

			this.currentStep =
				storedCurrent ?? null;

			this.workflowStatus =
				storedWorkflowStatus ?? "running";
		});
	}

	async fetch(request: Request): Promise<Response> {
		const upgradeHeader =
			request.headers.get("Upgrade");

		if (
			upgradeHeader?.toLowerCase() ===
			"websocket"
		) {
			const pair = new WebSocketPair();

			const [client, server] =
				Object.values(pair);

			this.ctx.acceptWebSocket(server);

			server.send(
				JSON.stringify(
					this.getStateMessage(),
				),
			);

			return new Response(null, {
				status: 101,
				webSocket: client,
			});
		}

		return new Response(
			"Expected WebSocket",
			{
				status: 400,
			},
		);
	}

	/**
	 * RPC method called by ProjectRunWorkflow.
	 *
	 * Repeating the same state transition is safe. Workflow notification calls
	 * occur outside step.do() and may therefore execute more than once.
	 */
	async updateStep(
		stepName: string,
		status: WorkflowStepStatus,
	): Promise<void> {
		this.stepStatuses.set(
			stepName,
			status,
		);

		if (status === "running") {
			this.currentStep = stepName;
			this.workflowStatus = "running";
		}

		if (status === "waiting") {
			this.currentStep = stepName;
			this.workflowStatus = "waiting";
		}

		if (status === "error") {
			this.currentStep = stepName;
			this.workflowStatus = "error";
		}

		const allCompleted =
			Array.from(
				this.stepStatuses.values(),
			).every(
				(stepStatus) =>
					stepStatus === "completed",
			);

		if (allCompleted) {
			this.workflowStatus = "completed";
			this.currentStep = null;
		} else if (
			status === "completed" &&
			this.currentStep === stepName
		) {
			this.currentStep = null;
		}

		await this.ctx.storage.put(
			"stepStatuses",
			Object.fromEntries(
				this.stepStatuses,
			),
		);

		await this.ctx.storage.put(
			"currentStep",
			this.currentStep,
		);

		await this.ctx.storage.put(
			"workflowStatus",
			this.workflowStatus,
		);

		this.broadcast(
			this.getStateMessage(),
		);
	}

	async webSocketMessage(
		ws: WebSocket,
		_message: string | ArrayBuffer,
	): Promise<void> {
		ws.send(
			JSON.stringify(
				this.getStateMessage(),
			),
		);
	}

	async webSocketClose(
		ws: WebSocket,
		code: number,
		reason: string,
		_wasClean: boolean,
	): Promise<void> {
		ws.close(code, reason);
	}

	private broadcast(
		message: Record<string, unknown>,
	): void {
		const sockets =
			this.ctx.getWebSockets();

		const json =
			JSON.stringify(message);

		for (const socket of sockets) {
			try {
				socket.send(json);
			} catch {
				/**
				 * A disconnected observer must not affect durable Workflow state.
				 */
			}
		}
	}

	private getStateMessage(): Record<
		string,
		unknown
	> {
		return {
			type: "workflow_update",
			currentStep: this.currentStep,
			stepStatuses:
				Object.fromEntries(
					this.stepStatuses,
				),
			workflowStatus:
				this.workflowStatus,
			timestamp: Date.now(),
		};
	}
}

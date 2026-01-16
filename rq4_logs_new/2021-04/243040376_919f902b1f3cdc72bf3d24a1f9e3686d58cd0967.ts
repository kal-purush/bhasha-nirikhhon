import * as express from "express";

function loggingMiddleware(): void {
	const app = express();
	app.use((req, res, next) => {
		const traceId = req.get("x-cloud-trace-context");
		const executionId = req.get("function-execution-id");
		setTraceIds(traceId, executionId);
		next();
	});
}

loggingMiddleware();

let traceIds;

function setTraceIds(traceId: string, executionId: string): void {
	traceIds = {
		traceId,
		executionId,
	};
}

export function clearTraceIds(): void {
	traceIds = undefined;
}

export function getTraceIds(): { traceId: string; executionId: string } {
	return traceIds;
}
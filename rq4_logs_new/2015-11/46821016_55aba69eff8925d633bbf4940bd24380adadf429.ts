declare module "@credo/queue-worker" {
    
    // INTERFACES
    // --------------------------------------------------------------------------------------------
	export interface WorkerOptions {
		minInterval?		: number;
		maxInterval?		: number;
		maxConcurrentJobs?	: number;
		maxRetries?			: number;
		logger?				: Logger;
	}
	
	export interface Logger {
		(message: string): void;
	}
	
	export interface JobHandler {
		(job: any, sent?: number, attempt?: number): Promise<any>;
	}
	
    // WORKER
    // --------------------------------------------------------------------------------------------
	export class Worker {
		constructor(client: any, queue: string, handler: JobHandler, options: WorkerOptions);
		start();
	}
}
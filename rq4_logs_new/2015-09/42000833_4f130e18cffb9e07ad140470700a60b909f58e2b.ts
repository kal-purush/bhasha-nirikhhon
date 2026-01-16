import * as child from "typed-child_process";
import * as events from "typed-events";

export interface ClusterSettings {
	exec?: string;
	args?: string[];
	silent?: boolean;
}

export class Worker extends events.EventEmitter {
	id: string;
	process: child.ChildProcess;
	suicide: boolean;
	send(message: any, sendHandle?: any): void;
	kill(signal?: string): void;
	destroy(signal?: string): void;
	disconnect(): void;
}

export var settings: ClusterSettings;
export var isMaster: boolean;
export var isWorker: boolean;
export function setupMaster(settings?: ClusterSettings): void;
export function fork(env?: any): Worker;
export function disconnect(callback?: Function): void;
export var worker: Worker;
export var workers: Worker[];

// Event emitter
export function addListener(event: string, listener: Function): void;
export function on(event: string, listener: Function): any;
export function once(event: string, listener: Function): void;
export function removeListener(event: string, listener: Function): void;
export function removeAllListeners(event?: string): void;
export function setMaxListeners(n: number): void;
export function listeners(event: string): Function[];
export function emit(event: string, ...args: any[]): boolean;
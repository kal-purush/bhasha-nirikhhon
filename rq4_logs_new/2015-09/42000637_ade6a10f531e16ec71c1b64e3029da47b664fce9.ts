import * as events from "typed-events";
import * as stream from "typed-stream";
import "typed-node";

export interface ChildProcess extends events.EventEmitter {
	stdin:  stream.Writable;
	stdout: stream.Readable;
	stderr: stream.Readable;
	pid: number;
	kill(signal?: string): void;
	send(message: any, sendHandle?: any): void;
	disconnect(): void;
	unref(): void;
}

export function spawn(command: string, args?: string[], options?: {
	cwd?: string;
	stdio?: any;
	custom?: any;
	env?: any;
	detached?: boolean;
}): ChildProcess;
export function exec(command: string, options: {
	cwd?: string;
	stdio?: any;
	customFds?: any;
	env?: any;
	encoding?: string;
	timeout?: number;
	maxBuffer?: number;
	killSignal?: string;
}, callback?: (error: Error, stdout: Buffer, stderr: Buffer) =>void ): ChildProcess;
export function exec(command: string, callback?: (error: Error, stdout: Buffer, stderr: Buffer) =>void ): ChildProcess;
export function execFile(file: string,
	callback?: (error: Error, stdout: Buffer, stderr: Buffer) =>void ): ChildProcess;
export function execFile(file: string, args?: string[],
	callback?: (error: Error, stdout: Buffer, stderr: Buffer) =>void ): ChildProcess;
export function execFile(file: string, args?: string[], options?: {
	cwd?: string;
	stdio?: any;
	customFds?: any;
	env?: any;
	encoding?: string;
	timeout?: number;
	maxBuffer?: string;
	killSignal?: string;
}, callback?: (error: Error, stdout: Buffer, stderr: Buffer) =>void ): ChildProcess;
export function fork(modulePath: string, args?: string[], options?: {
	cwd?: string;
	env?: any;
	encoding?: string;
}): ChildProcess;
export function execSync(command: string, options?: {
	cwd?: string;
	input?: string|Buffer;
	stdio?: any;
	env?: any;
	uid?: number;
	gid?: number;
	timeout?: number;
	maxBuffer?: number;
	killSignal?: string;
	encoding?: string;
}): ChildProcess;
export function execFileSync(command: string, args?: string[], options?: {
	cwd?: string;
	input?: string|Buffer;
	stdio?: any;
	env?: any;
	uid?: number;
	gid?: number;
	timeout?: number;
	maxBuffer?: number;
	killSignal?: string;
	encoding?: string;
}): ChildProcess;
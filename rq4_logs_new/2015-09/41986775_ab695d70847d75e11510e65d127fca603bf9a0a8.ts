import * as events from "typed-events";

export interface Stream extends events.EventEmitter {
	pipe<T extends WritableStream>(destination: T, options?: { end?: boolean; }): T;
}

export interface ReadableOptions {
	highWaterMark?: number;
	encoding?: string;
	objectMode?: boolean;
}

export class Readable extends events.EventEmitter implements ReadableStream {
	readable: boolean;
	constructor(opts?: ReadableOptions);
	_read(size: number): void;
	read(size?: number): string|Buffer;
	setEncoding(encoding: string): void;
	pause(): void;
	resume(): void;
	pipe<T extends WritableStream>(destination: T, options?: { end?: boolean; }): T;
	unpipe<T extends WritableStream>(destination?: T): void;
	unshift(chunk: string): void;
	unshift(chunk: Buffer): void;
	wrap(oldStream: ReadableStream): ReadableStream;
	push(chunk: any, encoding?: string): boolean;
}

export interface WritableOptions {
	highWaterMark?: number;
	decodeStrings?: boolean;
}

export class Writable extends events.EventEmitter implements WritableStream {
	writable: boolean;
	constructor(opts?: WritableOptions);
	_write(data: Buffer, encoding: string, callback: Function): void;
	_write(data: string, encoding: string, callback: Function): void;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
}

export interface DuplexOptions extends ReadableOptions, WritableOptions {
	allowHalfOpen?: boolean;
}

// Note: Duplex extends both Readable and Writable.
export class Duplex extends Readable implements ReadWriteStream {
	writable: boolean;
	constructor(opts?: DuplexOptions);
	_write(data: Buffer, encoding: string, callback: Function): void;
	_write(data: string, encoding: string, callback: Function): void;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
}

export interface TransformOptions extends ReadableOptions, WritableOptions {}

// Note: Transform lacks the _read and _write methods of Readable/Writable.
export class Transform extends events.EventEmitter implements ReadWriteStream {
	readable: boolean;
	writable: boolean;
	constructor(opts?: TransformOptions);
	_transform(chunk: Buffer, encoding: string, callback: Function): void;
	_transform(chunk: string, encoding: string, callback: Function): void;
	_flush(callback: Function): void;
	read(size?: number): any;
	setEncoding(encoding: string): void;
	pause(): void;
	resume(): void;
	pipe<T extends WritableStream>(destination: T, options?: { end?: boolean; }): T;
	unpipe<T extends WritableStream>(destination?: T): void;
	unshift(chunk: string): void;
	unshift(chunk: Buffer): void;
	wrap(oldStream: ReadableStream): ReadableStream;
	push(chunk: any, encoding?: string): boolean;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
}

export class PassThrough extends Transform {}
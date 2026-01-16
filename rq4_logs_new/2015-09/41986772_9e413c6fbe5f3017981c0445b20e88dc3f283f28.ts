/************************************************
*                                               *
*               Node.js v0.12.0 API             *
*                                               *
************************************************/

// compat for TypeScript 1.5.3
// if you use with --target es3 or --target es5 and use below definitions,
// use the lib.es6.d.ts that is bundled with TypeScript 1.5.3.
interface MapConstructor {}
interface WeakMapConstructor {}
interface SetConstructor {}
interface WeakSetConstructor {}

/************************************************
*                                               *
*                   GLOBAL                      *
*                                               *
************************************************/
declare var process: Process;
declare var global: Global;

declare var __filename: string;
declare var __dirname: string;

declare function setTimeout(callback: (...args: any[]) => void, ms: number, ...args: any[]): Timer;
declare function clearTimeout(timeoutId: Timer): void;
declare function setInterval(callback: (...args: any[]) => void, ms: number, ...args: any[]): Timer;
declare function clearInterval(intervalId: Timer): void;
declare function setImmediate(callback: (...args: any[]) => void, ...args: any[]): any;
declare function clearImmediate(immediateId: any): void;

interface NodeRequireFunction {
    (id: string): any;
}

interface NodeRequire extends NodeRequireFunction {
    resolve(id:string): string;
    cache: any;
    extensions: any;
    main: any;
}

declare var require: NodeRequire;

interface NodeModule {
    exports: any;
    require: NodeRequireFunction;
    id: string;
    filename: string;
    loaded: boolean;
    parent: any;
    children: any[];
}

declare var module: NodeModule;

// Same as module.exports
declare var exports: any;
declare var SlowBuffer: {
    new (str: string, encoding?: string): Buffer;
    new (size: number): Buffer;
    new (size: Uint8Array): Buffer;
    new (array: any[]): Buffer;
    prototype: Buffer;
    isBuffer(obj: any): boolean;
    byteLength(string: string, encoding?: string): number;
    concat(list: Buffer[], totalLength?: number): Buffer;
};


// Buffer class
interface Buffer extends NodeBuffer {}

/**
 * Raw data is stored in instances of the Buffer class.
 * A Buffer is similar to an array of integers but corresponds to a raw memory allocation outside the V8 heap.  A Buffer cannot be resized.
 * Valid string encodings: 'ascii'|'utf8'|'utf16le'|'ucs2'(alias of 'utf16le')|'base64'|'binary'(deprecated)|'hex'
 */
declare var Buffer: {
    /**
     * Allocates a new buffer containing the given {str}.
     *
     * @param str String to store in buffer.
     * @param encoding encoding to use, optional.  Default is 'utf8'
     */
    new (str: string, encoding?: string): Buffer;
    /**
     * Allocates a new buffer of {size} octets.
     *
     * @param size count of octets to allocate.
     */
    new (size: number): Buffer;
    /**
     * Allocates a new buffer containing the given {array} of octets.
     *
     * @param array The octets to store.
     */
    new (array: Uint8Array): Buffer;
    /**
     * Allocates a new buffer containing the given {array} of octets.
     *
     * @param array The octets to store.
     */
    new (array: any[]): Buffer;
    prototype: Buffer;
    /**
     * Returns true if {obj} is a Buffer
     *
     * @param obj object to test.
     */
    isBuffer(obj: any): boolean;
    /**
     * Returns true if {encoding} is a valid encoding argument.
     * Valid string encodings in Node 0.12: 'ascii'|'utf8'|'utf16le'|'ucs2'(alias of 'utf16le')|'base64'|'binary'(deprecated)|'hex'
     *
     * @param encoding string to test.
     */
    isEncoding(encoding: string): boolean;
    /**
     * Gives the actual byte length of a string. encoding defaults to 'utf8'.
     * This is not the same as String.prototype.length since that returns the number of characters in a string.
     *
     * @param string string to test.
     * @param encoding encoding used to evaluate (defaults to 'utf8')
     */
    byteLength(string: string, encoding?: string): number;
    /**
     * Returns a buffer which is the result of concatenating all the buffers in the list together.
     *
     * If the list has no items, or if the totalLength is 0, then it returns a zero-length buffer.
     * If the list has exactly one item, then the first item of the list is returned.
     * If the list has more than one item, then a new Buffer is created.
     *
     * @param list An array of Buffer objects to concatenate
     * @param totalLength Total length of the buffers when concatenated.
     *   If totalLength is not provided, it is read from the buffers in the list. However, this adds an additional loop to the function, so it is faster to provide the length explicitly.
     */
    concat(list: Buffer[], totalLength?: number): Buffer;
    /**
     * The same as buf1.compare(buf2).
     */
    compare(buf1: Buffer, buf2: Buffer): number;
};

/************************************************
*                                               *
*               GLOBAL INTERFACES               *
*                                               *
************************************************/
declare interface ErrnoException extends Error {
	errno?: number;
	code?: string;
	path?: string;
	syscall?: string;
	stack?: string;
}

declare interface EventEmitter {
	addListener(event: string, listener: Function): EventEmitter;
	on(event: string, listener: Function): EventEmitter;
	once(event: string, listener: Function): EventEmitter;
	removeListener(event: string, listener: Function): EventEmitter;
	removeAllListeners(event?: string): EventEmitter;
	setMaxListeners(n: number): void;
	listeners(event: string): Function[];
	emit(event: string, ...args: any[]): boolean;
}

declare interface ReadableStream extends EventEmitter {
	readable: boolean;
	read(size?: number): string|Buffer;
	setEncoding(encoding: string): void;
	pause(): void;
	resume(): void;
	pipe<T extends WritableStream>(destination: T, options?: { end?: boolean; }): T;
	unpipe<T extends WritableStream>(destination?: T): void;
	unshift(chunk: string): void;
	unshift(chunk: Buffer): void;
	wrap(oldStream: ReadableStream): ReadableStream;
}

declare interface WritableStream extends EventEmitter {
	writable: boolean;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
}

declare interface ReadWriteStream extends ReadableStream, WritableStream {}

declare interface Process extends EventEmitter {
	stdout: WritableStream;
	stderr: WritableStream;
	stdin: ReadableStream;
	argv: string[];
	execPath: string;
	abort(): void;
	chdir(directory: string): void;
	cwd(): string;
	env: any;
	exit(code?: number): void;
	getgid(): number;
	setgid(id: number): void;
	setgid(id: string): void;
	getuid(): number;
	setuid(id: number): void;
	setuid(id: string): void;
	version: string;
	versions: {
		http_parser: string;
		node: string;
		v8: string;
		ares: string;
		uv: string;
		zlib: string;
		openssl: string;
	};
	config: {
		target_defaults: {
			cflags: any[];
			default_configuration: string;
			defines: string[];
			include_dirs: string[];
			libraries: string[];
		};
		variables: {
			clang: number;
			host_arch: string;
			node_install_npm: boolean;
			node_install_waf: boolean;
			node_prefix: string;
			node_shared_openssl: boolean;
			node_shared_v8: boolean;
			node_shared_zlib: boolean;
			node_use_dtrace: boolean;
			node_use_etw: boolean;
			node_use_openssl: boolean;
			target_arch: string;
			v8_no_strict_aliasing: number;
			v8_use_snapshot: boolean;
			visibility: string;
		};
	};
	kill(pid: number, signal?: string): void;
	pid: number;
	title: string;
	arch: string;
	platform: string;
	memoryUsage(): { rss: number; heapTotal: number; heapUsed: number; };
	nextTick(callback: Function): void;
	umask(mask?: number): number;
	uptime(): number;
	hrtime(time?:number[]): number[];

	// Worker
	send?(message: any, sendHandle?: any): void;
}

declare interface Global {
	Array: typeof Array;
	ArrayBuffer: typeof ArrayBuffer;
	Boolean: typeof Boolean;
	Buffer: typeof Buffer;
	DataView: typeof DataView;
	Date: typeof Date;
	Error: typeof Error;
	EvalError: typeof EvalError;
	Float32Array: typeof Float32Array;
	Float64Array: typeof Float64Array;
	Function: typeof Function;
	GLOBAL: Global;
	Infinity: typeof Infinity;
	Int16Array: typeof Int16Array;
	Int32Array: typeof Int32Array;
	Int8Array: typeof Int8Array;
	Intl: typeof Intl;
	JSON: typeof JSON;
	Map: MapConstructor;
	Math: typeof Math;
	NaN: typeof NaN;
	Number: typeof Number;
	Object: typeof Object;
	Promise: Function;
	RangeError: typeof RangeError;
	ReferenceError: typeof ReferenceError;
	RegExp: typeof RegExp;
	Set: SetConstructor;
	String: typeof String;
	Symbol: Function;
	SyntaxError: typeof SyntaxError;
	TypeError: typeof TypeError;
	URIError: typeof URIError;
	Uint16Array: typeof Uint16Array;
	Uint32Array: typeof Uint32Array;
	Uint8Array: typeof Uint8Array;
	Uint8ClampedArray: Function;
	WeakMap: WeakMapConstructor;
	WeakSet: WeakSetConstructor;
	clearImmediate: (immediateId: any) => void;
	clearInterval: (intervalId: Timer) => void;
	clearTimeout: (timeoutId: Timer) => void;
	console: typeof console;
	decodeURI: typeof decodeURI;
	decodeURIComponent: typeof decodeURIComponent;
	encodeURI: typeof encodeURI;
	encodeURIComponent: typeof encodeURIComponent;
	escape: (str: string) => string;
	eval: typeof eval;
	global: Global;
	isFinite: typeof isFinite;
	isNaN: typeof isNaN;
	parseFloat: typeof parseFloat;
	parseInt: typeof parseInt;
	process: Process;
	root: Global;
	setImmediate: (callback: (...args: any[]) => void, ...args: any[]) => any;
	setInterval: (callback: (...args: any[]) => void, ms: number, ...args: any[]) => Timer;
	setTimeout: (callback: (...args: any[]) => void, ms: number, ...args: any[]) => Timer;
	undefined: typeof undefined;
	unescape: (str: string) => string;
	gc: () => void;
}

declare interface Timer {
	ref() : void;
	unref() : void;
}

/**
 * @deprecated
 */
interface NodeBuffer {
    [index: number]: number;
    write(string: string, offset?: number, length?: number, encoding?: string): number;
    toString(encoding?: string, start?: number, end?: number): string;
    toJSON(): any;
    length: number;
    equals(otherBuffer: Buffer): boolean;
    compare(otherBuffer: Buffer): number;
    copy(targetBuffer: Buffer, targetStart?: number, sourceStart?: number, sourceEnd?: number): number;
    slice(start?: number, end?: number): Buffer;
    writeUIntLE(value: number, offset: number, byteLength: number, noAssert?: boolean): number;
    writeUIntBE(value: number, offset: number, byteLength: number, noAssert?: boolean): number;
    writeIntLE(value: number, offset: number, byteLength: number, noAssert?: boolean): number;
    writeIntBE(value: number, offset: number, byteLength: number, noAssert?: boolean): number;
    readUIntLE(offset: number, byteLength: number, noAssert?: boolean): number;
    readUIntBE(offset: number, byteLength: number, noAssert?: boolean): number;
    readIntLE(offset: number, byteLength: number, noAssert?: boolean): number;
    readIntBE(offset: number, byteLength: number, noAssert?: boolean): number;
    readUInt8(offset: number, noAsset?: boolean): number;
    readUInt16LE(offset: number, noAssert?: boolean): number;
    readUInt16BE(offset: number, noAssert?: boolean): number;
    readUInt32LE(offset: number, noAssert?: boolean): number;
    readUInt32BE(offset: number, noAssert?: boolean): number;
    readInt8(offset: number, noAssert?: boolean): number;
    readInt16LE(offset: number, noAssert?: boolean): number;
    readInt16BE(offset: number, noAssert?: boolean): number;
    readInt32LE(offset: number, noAssert?: boolean): number;
    readInt32BE(offset: number, noAssert?: boolean): number;
    readFloatLE(offset: number, noAssert?: boolean): number;
    readFloatBE(offset: number, noAssert?: boolean): number;
    readDoubleLE(offset: number, noAssert?: boolean): number;
    readDoubleBE(offset: number, noAssert?: boolean): number;
    writeUInt8(value: number, offset: number, noAssert?: boolean): void;
    writeUInt16LE(value: number, offset: number, noAssert?: boolean): void;
    writeUInt16BE(value: number, offset: number, noAssert?: boolean): void;
    writeUInt32LE(value: number, offset: number, noAssert?: boolean): void;
    writeUInt32BE(value: number, offset: number, noAssert?: boolean): void;
    writeInt8(value: number, offset: number, noAssert?: boolean): void;
    writeInt16LE(value: number, offset: number, noAssert?: boolean): void;
    writeInt16BE(value: number, offset: number, noAssert?: boolean): void;
    writeInt32LE(value: number, offset: number, noAssert?: boolean): void;
    writeInt32BE(value: number, offset: number, noAssert?: boolean): void;
    writeFloatLE(value: number, offset: number, noAssert?: boolean): void;
    writeFloatBE(value: number, offset: number, noAssert?: boolean): void;
    writeDoubleLE(value: number, offset: number, noAssert?: boolean): void;
    writeDoubleBE(value: number, offset: number, noAssert?: boolean): void;
    fill(value: any, offset?: number, end?: number): void;
}
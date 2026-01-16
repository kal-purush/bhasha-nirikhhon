import * as stream from "typed-stream";
import "typed-node";

export interface Socket extends stream.Duplex {
	// Extended base methods
	write(buffer: Buffer): boolean;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	write(str: string, encoding?: string, fd?: string): boolean;

	connect(port: number, host?: string, connectionListener?: Function): void;
	connect(path: string, connectionListener?: Function): void;
	bufferSize: number;
	setEncoding(encoding?: string): void;
	write(data: any, encoding?: string, callback?: Function): void;
	destroy(): void;
	pause(): void;
	resume(): void;
	setTimeout(timeout: number, callback?: Function): void;
	setNoDelay(noDelay?: boolean): void;
	setKeepAlive(enable?: boolean, initialDelay?: number): void;
	address(): { port: number; family: string; address: string; };
	unref(): void;
	ref(): void;

	remoteAddress: string;
	remoteFamily: string;
	remotePort: number;
	localAddress: string;
	localPort: number;
	bytesRead: number;
	bytesWritten: number;

	// Extended base methods
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
	end(data?: any, encoding?: string): void;
}

export var Socket: {
	new (options?: { fd?: string; type?: string; allowHalfOpen?: boolean; }): Socket;
};

export interface Server extends Socket {
	listen(port: number, host?: string, backlog?: number, listeningListener?: Function): Server;
	listen(path: string, listeningListener?: Function): Server;
	listen(handle: any, listeningListener?: Function): Server;
	close(callback?: Function): Server;
	address(): { port: number; family: string; address: string; };
	maxConnections: number;
	connections: number;
}
export function createServer(connectionListener?: (socket: Socket) =>void ): Server;
export function createServer(options?: { allowHalfOpen?: boolean; }, connectionListener?: (socket: Socket) =>void ): Server;
export function connect(options: { allowHalfOpen?: boolean; }, connectionListener?: Function): Socket;
export function connect(port: number, host?: string, connectionListener?: Function): Socket;
export function connect(path: string, connectionListener?: Function): Socket;
export function createConnection(options: { allowHalfOpen?: boolean; }, connectionListener?: Function): Socket;
export function createConnection(port: number, host?: string, connectionListener?: Function): Socket;
export function createConnection(path: string, connectionListener?: Function): Socket;
export function isIP(input: string): number;
export function isIPv4(input: string): boolean;
export function isIPv6(input: string): boolean;
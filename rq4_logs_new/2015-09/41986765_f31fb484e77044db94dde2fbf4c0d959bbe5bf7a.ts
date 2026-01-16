import * as events from "typed-events";
import * as net from "typed-net";
import * as stream from "typed-stream";
import "typed-node";

export interface Server extends events.EventEmitter {
	listen(port: number, hostname?: string, backlog?: number, callback?: Function): Server;
	listen(port: number, hostname?: string, callback?: Function): Server;
	listen(path: string, callback?: Function): Server;
	listen(handle: any, listeningListener?: Function): Server;
	close(cb?: any): Server;
	address(): { port: number; family: string; address: string; };
	maxHeadersCount: number;
}
/**
	* @deprecated Use IncomingMessage
	*/
export interface ServerRequest extends IncomingMessage {
	connection: net.Socket;
}
export interface ServerResponse extends events.EventEmitter, stream.Writable {
	// Extended base methods
	write(buffer: Buffer): boolean;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	write(str: string, encoding?: string, fd?: string): boolean;

	writeContinue(): void;
	writeHead(statusCode: number, reasonPhrase?: string, headers?: any): void;
	writeHead(statusCode: number, headers?: any): void;
	statusCode: number;
	statusMessage: string;
	setHeader(name: string, value: string): void;
	sendDate: boolean;
	getHeader(name: string): string;
	removeHeader(name: string): void;
	write(chunk: any, encoding?: string): any;
	addTrailers(headers: any): void;

	// Extended base methods
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
	end(data?: any, encoding?: string): void;
}
export interface ClientRequest extends events.EventEmitter, stream.Writable {
	// Extended base methods
	write(buffer: Buffer): boolean;
	write(buffer: Buffer, cb?: Function): boolean;
	write(str: string, cb?: Function): boolean;
	write(str: string, encoding?: string, cb?: Function): boolean;
	write(str: string, encoding?: string, fd?: string): boolean;

	write(chunk: any, encoding?: string): void;
	abort(): void;
	setTimeout(timeout: number, callback?: Function): void;
	setNoDelay(noDelay?: boolean): void;
	setSocketKeepAlive(enable?: boolean, initialDelay?: number): void;

	// Extended base methods
	end(): void;
	end(buffer: Buffer, cb?: Function): void;
	end(str: string, cb?: Function): void;
	end(str: string, encoding?: string, cb?: Function): void;
	end(data?: any, encoding?: string): void;
}
export interface IncomingMessage extends events.EventEmitter, stream.Readable {
	httpVersion: string;
	headers: any;
	rawHeaders: string[];
	trailers: any;
	rawTrailers: any;
	setTimeout(msecs: number, callback: Function): Timer;
	/**
		* Only valid for request obtained from http.Server.
		*/
	method?: string;
	/**
		* Only valid for request obtained from http.Server.
		*/
	url?: string;
	/**
		* Only valid for response obtained from http.ClientRequest.
		*/
	statusCode?: number;
	/**
		* Only valid for response obtained from http.ClientRequest.
		*/
	statusMessage?: string;
	socket: net.Socket;
}
/**
	* @deprecated Use IncomingMessage
	*/
export interface ClientResponse extends IncomingMessage { }

export interface AgentOptions {
	/**
		* Keep sockets around in a pool to be used by other requests in the future. Default = false
		*/
	keepAlive?: boolean;
	/**
		* When using HTTP KeepAlive, how often to send TCP KeepAlive packets over sockets being kept alive. Default = 1000.
		* Only relevant if keepAlive is set to true.
		*/
	keepAliveMsecs?: number;
	/**
		* Maximum number of sockets to allow per host. Default for Node 0.10 is 5, default for Node 0.12 is Infinity
		*/
	maxSockets?: number;
	/**
		* Maximum number of sockets to leave open in a free state. Only relevant if keepAlive is set to true. Default = 256.
		*/
	maxFreeSockets?: number;
}

export class Agent {
	maxSockets: number;
	sockets: any;
	requests: any;

	constructor(opts?: AgentOptions);

	/**
		* Destroy any sockets that are currently in use by the agent.
		* It is usually not necessary to do this. However, if you are using an agent with KeepAlive enabled,
		* then it is best to explicitly shut down the agent when you know that it will no longer be used. Otherwise,
		* sockets may hang open for quite a long time before the server terminates them.
		*/
	destroy(): void;
}

export var METHODS: string[];

export var STATUS_CODES: {
	[errorCode: number]: string;
	[errorCode: string]: string;
};
export function createServer(requestListener?: (request: IncomingMessage, response: ServerResponse) =>void ): Server;
export function createClient(port?: number, host?: string): any;
export function request(options: any, callback?: (res: IncomingMessage) => void): ClientRequest;
export function get(options: any, callback?: (res: IncomingMessage) => void): ClientRequest;
export var globalAgent: Agent;
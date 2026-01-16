import {Socket} from "net";
import {IDefer, defer} from "./defer";

const EOL = new Buffer([0x0d, 0x0a]);
const EOD = new Buffer([0x0d, 0x0a, 0x2e, 0x0d, 0x0a]);

export interface ILoopEvent {
	command?: string,
	overflow?: boolean,
	data?: Buffer,
	eod?: boolean
}

export class Loop {
	private _sock: Socket;
	private _tail: Buffer;
	private _d: IDefer;
	private _overflow: boolean;

	constructor(sock: Socket) {
		this._sock = sock;
		this._sock.once("error", err => {
			if (!this._d)
				this._d = defer();
			if (this._d.pending)
				this._d.reject(err);
		});
	}

	public next(options?: { data?: boolean }): Promise<ILoopEvent> {
		// process options
		options = Object.assign({ data: false }, options);

		// if we already have a promise in pending state - throw error
		// that is calls to next() must be serialized
		if (this.isPending)
			throw new Error("Already in progress.");

		if (this._d)
			return this._d.promise;

		this._d = defer();

		// try to process buffered data first
		if (this._tail)
			this._tail = this.processChunk(this._tail, options.data);

		// if no data has been processed - receive and process more data
		if (this.isPending)
			this._sock.once("readable", this.sock_readable.bind(this, options));

		return this._d.promise;
	}

	private get isPending(): boolean {
		return this._d && this._d.pending;
	}

	private resolve(value: ILoopEvent) {
		this._d.resolve(value);
		this._overflow = false;
		if (value != null)
			this._d = null;
	}

	private sock_readable(options) {
		let chunk = this._sock.read();

		if (!chunk) {
			this.resolve(null);
			return;
		}

		if (this._tail) {
			chunk = chunk ? Buffer.concat([this._tail, chunk], this._tail.length + chunk.length) : this._tail;
			this._tail = null;
		}

		let tail = this.processChunk(chunk, options.data);

		if (tail.length)
			this._tail = tail;

		// if no data has been processed - receive and process more data
		if (this.isPending)
			this._sock.once("readable", this.sock_readable.bind(this, options));
	}

	private processChunk(chunk: Buffer, isData: boolean): Buffer {
		let index;
		if (isData) {
			let data: Buffer;
			let obj: any;
			index = chunk.indexOf(EOD);
			if (~index) {
				data = chunk.slice(0, index);
				chunk = chunk.slice(index + EOD.length);
				obj = { eod: true };
				if (data.length)
					obj.data = data;
			}
			else {
				// if no EOD found we must hold four last octets so we could successfully detect EOD when more data arrives
				data = chunk.slice(0, -4);
				chunk = chunk.slice(-4);
				if (data.length)
					obj = { data: data };
			}

			if (obj)
				this.resolve(obj);
		}
		else {
			index = chunk.indexOf(EOL);
			if (~index) {
				let result: ILoopEvent = {};
				if (this._overflow || index > 1024)
					result.overflow = true;
				else
					result.command = chunk.toString("utf8", 0, index);
				chunk = chunk.slice(index + EOL.length);
				this.resolve(result);
			}
			else {
				// if \r\n sequence not found we must ensure the buffer does not get too large
				// and if it is then we need to keep truncating it but take care of \r\n boundary
				if (this._overflow || chunk.length > 1025 || chunk.length === 1025 && chunk[1024] === 0xd) {
					this._overflow = true;
					if (chunk[chunk.length - 1] === 0xd)
						chunk = chunk.slice(-1);
					else
						chunk.slice(0, 0);
				}
			}
		}

		return chunk;
	}
}
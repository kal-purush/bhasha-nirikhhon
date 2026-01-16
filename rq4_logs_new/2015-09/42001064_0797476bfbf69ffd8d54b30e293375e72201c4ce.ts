/// <reference path="../typings/base64-js.d.ts" />
/// <reference path="../typings/text-encoder-lite.d.ts" />
import * as base64 from 'base64-js';
import { TextEncoderLite, TextDecoderLite } from 'text-encoder-lite-module';
import { SocketWrapper } from './mpd-protocol';
import { MPC } from './mpc';

export class WebSocketConnection implements SocketWrapper {

	private uri: string;
	private textEncoder: TextEncoderLite;
	private textDecoder: TextDecoderLite;
	private wsClient: WebSocket;
	
	constructor(uri: string) {
		this.uri = uri;
		this.textEncoder = new TextEncoderLite();
		this.textDecoder = new TextDecoderLite();
	}

	connect(receive: (msg: string) => void) {
		this.wsClient = new WebSocket(this.uri, ['base64']);
		this.wsClient.onmessage = (e) => receive(this.textDecoder.decode(base64.toByteArray(e.data)));
	}
	
	send(msg: string) {
		this.wsClient.send(base64.fromByteArray(this.textEncoder.encode(msg)));
	}
}

export function viaWebSocket(uri: string): MPC {
	return new MPC(new WebSocketConnection(uri));
}
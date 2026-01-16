import { NgModule, ModuleWithProviders, InjectionToken, Injectable, Inject } from "@angular/core";
import { SocketIO as Socket } from "../socketio";
import { isAndroid } from "tns-core-modules/platform/platform";

export interface IOOptions {
    compress: boolean;
    connectParams: { [key: string]: any };
    cookies: string[];
    extraHeaders: { [key: string]: any };
    forceNew: boolean;
    forcePolling: boolean;
    forceWebsockets: boolean;
    log: boolean;
    path: string;
    reconnects: boolean;
    reconnectAttempts: number;
    reconnectWait: number; // Time in milli seconds
    secure: boolean;
}

export type SocketIOOptions = Partial<IOOptions>;

export const SOCKETIO_URL = new InjectionToken("SOCKETIO_URL");
export const SOCKETIO_OPTIONS = new InjectionToken("SOCKETIO_OPTIONS");

@Injectable()
export class SocketIO {
    private _socket: Socket;

    constructor(
        @Inject(SOCKETIO_URL) url: string,
        @Inject(SOCKETIO_OPTIONS) options?: SocketIOOptions
    ) {
        this.socket = new Socket(url, options);
    }

    set socket(socket: Socket) {
        this._socket = socket;
    }

    get socket(): Socket {
        return this._socket;
    }

    on(event: string, callback: any): void {
        this.socket.on(event, callback);
    }

    emit(...args: any[]): void {
        this.socket.emit(...args);
    }

    joinNamespace(nsp: string): void {
        this.socket.joinNamespace(nsp);
    }

    leaveNamespace(): void {
        this.socket.leaveNamespace();
    }

    static serialize(value: any): any {
        if (isAndroid) {
            return Socket.serialize(value);
        }
        return value;
    }

    static deserialize(value: any): any {
        if (isAndroid) {
            return Socket.deserialize(value);
        }
        return value;
    }
}

@NgModule()
export class SocketIOModule {
    static forRoot(url: string, options?: SocketIOOptions): ModuleWithProviders {
        return {
            ngModule: SocketIOModule,
            providers: [
                SocketIO,
                { provide: SOCKETIO_URL, useValue: url },
                { provide: SOCKETIO_OPTIONS, useValue: options },
            ]
        };
    }
}
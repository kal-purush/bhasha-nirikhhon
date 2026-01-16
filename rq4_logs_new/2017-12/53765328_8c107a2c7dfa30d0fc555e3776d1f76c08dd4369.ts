import { ModuleWithProviders, InjectionToken } from "@angular/core";
import { SocketIO as Socket } from "../socketio";
export interface IOOptions {
    compress: boolean;
    connectParams: {
        [key: string]: any;
    };
    cookies: string[];
    extraHeaders: {
        [key: string]: any;
    };
    forceNew: boolean;
    forcePolling: boolean;
    forceWebsockets: boolean;
    log: boolean;
    path: string;
    reconnects: boolean;
    reconnectAttempts: number;
    reconnectWait: number;
    secure: boolean;
}
export declare type SocketIOOptions = Partial<IOOptions>;
export declare const SOCKETIO_URL: InjectionToken<{}>;
export declare const SOCKETIO_OPTIONS: InjectionToken<{}>;
export declare class SocketIO {
    private _socket;
    constructor(url: string, options?: SocketIOOptions);
    socket: Socket;
    on(event: string, callback: any): void;
    emit(...args: any[]): void;
    joinNamespace(nsp: string): void;
    leaveNamespace(): void;
    static serialize(value: any): any;
    static deserialize(value: any): any;
}
export declare class SocketIOModule {
    static forRoot(url: string, options?: SocketIOOptions): ModuleWithProviders;
}
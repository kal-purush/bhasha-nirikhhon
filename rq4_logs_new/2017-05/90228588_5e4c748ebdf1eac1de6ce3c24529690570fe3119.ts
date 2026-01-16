import { IWebSocketClient } from 'leancloud-typescript-rx-sdk';
export declare class BrowserWebSocketClient implements IWebSocketClient {
    onopen: (event: {
        target: BrowserWebSocketClient;
    }) => void;
    onerror: (err: Error) => void;
    onclose: (event: {
        wasClean: boolean;
        code: number;
        reason: string;
        target: BrowserWebSocketClient;
    }) => void;
    onmessage: (event: {
        data: any;
        type: string;
        target: BrowserWebSocketClient;
    }) => void;
    readyState: number;
    wsc: any;
    open(url: string, protocols?: string | string[]): void;
    close(code?: number, data?: any): void;
    send(data: ArrayBuffer | string | Blob): void;
}
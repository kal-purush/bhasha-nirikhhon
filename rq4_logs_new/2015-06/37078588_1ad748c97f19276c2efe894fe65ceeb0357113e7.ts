/// <reference path="tsd.d.ts" />
import {IncomingMessage, ServerResponse} from 'http';

declare module connect {
    interface ConnectResult {
        use(input: any): (request: IncomingMessage, response: ServerResponse) => void
    }

    export function connect(input: any): ConnectResult ;
}
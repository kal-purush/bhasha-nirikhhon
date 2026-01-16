/// <reference types="node" />
import { IncomingMessage, ServerResponse, ClientRequest, OutgoingHttpHeaders } from 'http';
import { Span } from '@opentelemetry/api';
import { HttpInstrumentationConfig } from '@opentelemetry/instrumentation-http';
export declare class HttpInstrumentation {
    static onRequestBody(span: Span, body: any): void;
    static onRequestHeaders(span: Span, headers: OutgoingHttpHeaders): void;
    static onResponseBody(span: Span, body: any): void;
    static onResponseHeaders(span: Span, headers: OutgoingHttpHeaders): void;
    static requestHook(span: Span, request: ClientRequest): void;
    static responseHook(span: Span, response: IncomingMessage | ServerResponse<IncomingMessage>): void;
    static instrumentationConfig(config?: HttpInstrumentationConfig): HttpInstrumentationConfig;
    static withPayloadDetails(): {
        '@opentelemetry/instrumentation-http': HttpInstrumentationConfig;
    };
}
export type JsonValue = string | number | boolean | null | undefined | {
    [key: string]: JsonValue;
} | JsonValue[];

export type CallFunction = (nativeMethod: string, parameter?: JsonValue) => void;
export type AsyncCallFunction = (nativeMethod: string, parameter?: JsonValue) => Promise<JsonValue>;
export type SyncCallFunction = (nativeMethod: string, parameter?: JsonValue) => void;

export type JsNormalFunction = (this: JsNormalFunctionHandler, ...parameter: JsonValue[]) => JsonValue;

export type AsyncPromiseFunction = (this: AsyncJsFunctionHandler, ...parameter: JsonValue[]) => Promise<JsonValue>;
export type AsyncCallbackFunction = (this: AsyncJsFunctionHandler, ...parameter: [...JsonValue[], (r: JsonValue) => void]) => any;
export type AsyncJsFunction = AsyncPromiseFunction | AsyncCallbackFunction;

export type ProcessJsFunction = (this: ProcessJsFunctionHandler, ...parameter: [...JsonValue[], (t: JsonValue, complete: boolean) => void]) => void;

export type JsNormalFunctionHandler = { [key: string]: JsNormalFunction } | JsNormalFunction;
export type AsyncJsFunctionHandler = { [key: string]: AsyncJsFunction } | AsyncJsFunction;
export type ProcessJsFunctionHandler = { [key: string]: ProcessJsFunction } | ProcessJsFunction;

export type RegisterFunction = (handlerName: string, handler: JsNormalFunctionHandler) => void;
export type RegisterAsyncFunction = (handlerName: string, handler: AsyncJsFunctionHandler) => void;
export type RegisterProcessFunction = (handlerName: string, handler: ProcessJsFunctionHandler) => void;
import {v4 as uuid} from "uuid";

type JsonValue = string | number | boolean | null | undefined | {
    [key: string]: JsonValue;
} | JsonValue[];

interface WebViewBridgeCall {
    call(nativeMethod: string, parameter?: JsonValue): void;
    asyncCall(nativeMethod: string, parameter?: JsonValue): Promise<JsonValue>;
    syncCall(nativeMethod: string, parameter?: JsonValue): JsonValue;
}

type JsReturnFunction = (this: JsNormalFunctionHandler, ...parameter: JsonValue[]) => JsonValue;

export type JsNormalFunction = JsReturnFunction;
export type JsNormalFunctionHandler = { [key: string]: JsNormalFunction };

type AsyncPromiseFunction = (this: AsyncJsFunctionHandler, ...parameter: JsonValue[]) => Promise<JsonValue>;
type AsyncCallbackFunction = (this: AsyncJsFunctionHandler, ...parameter: [...JsonValue[], (r: JsonValue) => void]) => any;

export type AsyncJsFunction = AsyncPromiseFunction | AsyncCallbackFunction;
export type AsyncJsFunctionHandler = { [key: string]: AsyncJsFunction };

export type ProcessJsFunction = (this: ProcessJsFunctionHandler, ...parameter: [...JsonValue[], (t: JsonValue, complete: boolean) => void]) => void;
export type ProcessJsFunctionHandler = { [key: string]: ProcessJsFunction };



interface WebViewRegister {
    register(handlerName: string, handler: JsNormalFunction | JsNormalFunctionHandler): void;
    registerAsync(handlerName: string, handler: AsyncJsFunction | AsyncJsFunctionHandler): void;
    registerProgress(handlerName: string, handler: ProcessJsFunction | ProcessJsFunctionHandler): void;
}

type RegisterMap = {
    normal: {
        [key: string]: JsNormalFunction | JsNormalFunctionHandler;
    },
    async: {
        [key: string]: AsyncJsFunction | AsyncJsFunctionHandler;
    },
    progress: {
        [key: string]: ProcessJsFunction | ProcessJsFunctionHandler;
    },
    [key: string]: {
        [key: string]: JsNormalFunction | JsNormalFunctionHandler | AsyncJsFunction | AsyncJsFunctionHandler | ProcessJsFunction | ProcessJsFunctionHandler
    };
}

declare global {
    interface Window {
        // 利用 dsbridge native 版本时，android native 注入的字段
        _dsbridge?: any;
        webkit?: any;
        _handleMessageFromNative: any;
    }
}
class WebViewBridge implements WebViewRegister, WebViewBridgeCall {
    private registerMap: RegisterMap = {
        normal: {},
        async: {},
        progress: {}
    }

    call(nativeMethod: string, parameter?: JsonValue): void {
        const arg = {data: parameter === undefined ? null : parameter};
        if (window._dsbridge) {
            window._dsbridge.call(nativeMethod, JSON.stringify(arg));
        } else {
            window.webkit.messageHandlers.asyncBridge.postMessage({
                method: nativeMethod,
                arg: JSON.stringify(arg),
            });
        }
    }

    asyncCall(nativeMethod: string, parameter?: JsonValue): Promise<JsonValue> {
        const arg = {data: parameter === undefined ? null : parameter};
        return new Promise<JsonValue>(resolve => {
            const callbackId = uuid();
            (window as any)[callbackId] = (result: JsonValue) => {
                resolve(result);
            }

            (arg as any)[callbackField] = callbackId;
            if (window._dsbridge) {
                window._dsbridge.call(nativeMethod, JSON.stringify(arg));
            } else {
                window.webkit.messageHandlers.asyncBridge.postMessage({
                    method: nativeMethod,
                    arg: JSON.stringify(arg),
                });
            }
        });
    }

    syncCall(nativeMethod: string, parameter?: JsonValue): JsonValue {
        const arg = {data: parameter === undefined ? null : parameter};
        const ret = "";
        if (window._dsbridge) {
            // android 只能传递 string 之类的原始类型
            window._dsbridge.call(nativeMethod, JSON.stringify(parameter));
        } else {
            prompt("_dsbridge=" + nativeMethod, JSON.stringify(arg));
        }
        return JSON.parse(ret||'{}').data;
    }

    register(handlerName: string, handler: JsNormalFunction | JsNormalFunctionHandler): void {
        this.registerMap.normal[handlerName] = handler;
    }

    registerAsync(handlerName: string, handler: AsyncJsFunction | AsyncJsFunctionHandler): void {
        this.registerMap.async[handlerName] = handler;
    }

    registerProgress(handlerName: string, handler: ProcessJsFunction | ProcessJsFunctionHandler): void {
        this.registerMap.progress[handlerName] = handler;
    }

    private splitNativeMethod(method: string): [namespace: string, method: string] {
        const methods = method.split(".");
        const func = methods.pop()!;
        const namespace = methods.join(".");
        return [namespace, func];
    }

    private getFunction(handlerName: string): {type: string, function: JsNormalFunction | AsyncJsFunction | ProcessJsFunction, obj: any} | undefined {

        for (const key in this.registerMap) {
            if (Object.prototype.hasOwnProperty.call(this.registerMap, key)) {
                const element = this.registerMap[key];
                const funOrObj = element[handlerName];
                if (funOrObj && typeof funOrObj === "function") {
                    return {type: key, function: funOrObj, obj: undefined};
                }
                const [namespace, func] = this.splitNativeMethod(handlerName);
                const obj = (element[namespace] || {});
                if (typeof obj === "object" && typeof obj[func] === "function") {
                    return {type: key, function: obj[func], obj};
                }
            }
        }
        return undefined;
    }

    handleMessageFromNative = (info: JsonValue) => {
        const {method, data, callbackId} = info as {method: string, data: string, callbackId: string};
        // dsBridge native 端将传入的参数（数组）转换成了 string，所以需要转换回来
        const args = JSON.parse(data) as [...JsonValue[]];
        const ret: ReturnType = {
            id: callbackId,
            complete: true,
            data: undefined,
        }

        const result = this.getFunction(method);
        if (!result) {
            return;
        }
        const {type, function: func, obj} = result;
        switch (type) {
            case "normal":{
                const normalFunc = func as JsNormalFunction;
                ret.data = normalFunc.apply(obj, args);
                this.call(returnFunction, ret);
                break;
            }
            case "async": {
                const asyncF = func as AsyncJsFunction;
                // check function is promise or not
                if ((asyncF as unknown as Promise<JsonValue>).then) {
                    const async1 = asyncF as AsyncPromiseFunction;
                    async1.apply(obj, args).then(r => {
                        ret.data = r;
                        this.call(returnFunction, ret);
                    }).catch(e => {
                        // TODO:
                    });
                } else {
                    const async1 = func as AsyncCallbackFunction;
                    const argsWithCallback = [...args, (r: JsonValue) => {
                        ret.data = r;
                        this.call(returnFunction, ret);
                    }] as [...JsonValue[], (r: JsonValue) => void];
                    async1.apply(obj, argsWithCallback);
                }
                break;
            }
            case "progress": {
                const processF = func as ProcessJsFunction;
                const argsWithProgress = [...args, (t: JsonValue, complete: boolean) => {
                    ret.data = t;
                    ret.complete = complete;
                    this.call(returnFunction, ret);
                }] as [...JsonValue[], (t: JsonValue, complete: boolean) => void];
                processF.apply(obj, argsWithProgress);
                break;
            }
        }
    }
}

const callbackField = "_dscbstub";
const returnFunction = "_dsb.returnValue";

type ReturnType = {
    id: string;
    complete: boolean;
    data: JsonValue | undefined;
}

export const bridge = new WebViewBridge();
window._handleMessageFromNative = bridge.handleMessageFromNative;
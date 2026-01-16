declare module "proxy-polyfill" {
    export interface ProxyHandler {
        get: (target: any, property: string, receiver: Proxy) => any,
        set: (target: any, property: string, value: any, receiver: Proxy) => boolean,
        apply: (target: any, thisArg: any, argumentsList: any[]) => any,
        construct: (target: any, argumentsList: any[], newTarget: any) => Object
    }
    export class Proxy {
        constructor(target: any, handler: ProxyHandler);
        public static revocable(target: any, handler: ProxyHandler): Proxy;
    }
}
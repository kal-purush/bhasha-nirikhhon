/// <reference path="../../node_modules/reflect-metadata/reflect-metadata.d.ts"/>
import { metaContainer } from "./metadata/MetaContainer";
import "reflect-metadata";

export function Controller(routePrefix: string): ClassDecorator;
export function Controller(target: Function): void;
export function Controller(param: any): any {
    if (typeof param === "function") {
        const target: Function = param;
        metaContainer.addControllerDecoratorMeta({
            name: "Controller",
            type: "routing",
            controllerClass: target,
            data: undefined
        });
    }
    if (typeof param === "string") {
        const routePrefix = param.toString();
        return (target: Function) => {
            metaContainer.addControllerDecoratorMeta({
                name: "Controller",
                type: "routing",
                controllerClass: target,
                data: {
                    routePrefix: routePrefix
                }
            });
        };
    }
}

export function Get(route: string | RegExp, returnType: Object): MethodDecorator {
    return (target: Object, propertyKey: string, descriptor: TypedPropertyDescriptor<any>) => {
        metaContainer.addActionDecoratorMeta({
            name: "Get",
            type: "routing",
            controllerPrototype: target,
            action: propertyKey,
            paramTypes: Reflect.getMetadata("design:paramtypes", target, propertyKey),
            data: {
                route: route,
                httpMethod: "get",
                returnType: returnType
            }
        });
    };
}

export function Post(route: string | RegExp, returnType: Object): MethodDecorator {
    return (target: Object, propertyKey: string, descriptor: TypedPropertyDescriptor<any>) => {
        metaContainer.addActionDecoratorMeta({
            name: "Post",
            type: "routing",
            controllerPrototype: target,
            action: propertyKey,
            paramTypes: Reflect.getMetadata("design:paramtypes", target, propertyKey),
            data: {
                route: route,
                httpMethod: "post",
                returnType: returnType
            }
        });
    };
}

export function Body(variableKey: string): ParameterDecorator;
export function Body(target: Object, propertyKey: string | symbol, parameterIndex: number): void;
export function Body(...params: any[]): any {
    if (params.length === 1 && typeof params[0] === "string") {
        const variableKey = params[0];
        return (target: Object, propertyKey: string | symbol, parameterIndex: number): void => {
            const paramTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey);
            metaContainer.addParamDecoratorMeta({
                name: "Body",
                type: "routing",
                controllerPrototype: target,
                action: propertyKey.toString(),
                index: parameterIndex,
                paramType: paramTypes[parameterIndex],
                data: {
                    variableKey: variableKey
                }
            });
        };
    }

    if (params.length === 3) {
        const [target, propertyKey, parameterIndex] = params;
        const paramTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey);
        metaContainer.addParamDecoratorMeta({
            name: "Body",
            type: "routing",
            controllerPrototype: target,
            action: propertyKey.toString(),
            index: parameterIndex,
            paramType: paramTypes[parameterIndex],
            data: undefined
        });
        return;
    }
    return;
}

export function Query(variableKey: string): ParameterDecorator {
    return (target: Object, propertyKey: string | symbol, parameterIndex: number): void => {
        const paramTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey);
        metaContainer.addParamDecoratorMeta({
            name: "Query",
            type: "routing",
            controllerPrototype: target,
            action: propertyKey.toString(),
            index: parameterIndex,
            paramType: paramTypes[parameterIndex],
            data: {
                variableKey
            }
        });
    };
}

export function AuthorizeAll(...users: string[]): ClassDecorator;
export function AuthorizeAll(target: Function): void;
export function AuthorizeAll(...params: any[]): any {
    if (params.length >= 1 && params.every(p => typeof p === "string")) {
        return (target: Function) => {
            metaContainer.addControllerDecoratorMeta({
                name: "AuthorizeAll",
                type: "authentication",
                controllerClass: target,
                data: {
                    users: params
                }
            });
        };
    }

    if (params.length === 1 && typeof params[0] === "function") {
        const target = params[0];
        metaContainer.addControllerDecoratorMeta({
            name: "AuthorizeAll",
            type: "authentication",
            controllerClass: target,
            data: undefined
        });
    }
}

export function Authorize(...users: string[]): MethodDecorator;
export function Authorize(target: Object, propertyKey: string, descriptor: TypedPropertyDescriptor<any>): void;
export function Authorize(...params: any[]): any {
    if (params.length >= 1 && params.every(p => typeof p === "string")) {
        return (target: Object, propertyKey: string, descriptor: TypedPropertyDescriptor<any>) => {
            metaContainer.addActionDecoratorMeta({
                name: "Get",
                type: "routing",
                controllerPrototype: target,
                action: propertyKey,
                paramTypes: Reflect.getMetadata("design:paramtypes", target, propertyKey),
                data: {
                    users: params
                }
            });
        };
    }

    if (params.length === 3
        && typeof params[0] === "function"
        && typeof params[1] === "string"
        && typeof params[2] === "object"
    ) {
        const [target, propertyKey] = params;
        const paramTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey);
        metaContainer.addActionDecoratorMeta({
            name: "Authorize",
            type: "authentication",
            controllerPrototype: target,
            action: propertyKey.toString(),
            paramTypes: paramTypes,
            data: undefined
        });
        return;
    }
}

export function AllowAnonymous(target: Object, propertyKey: string, descripter: TypedPropertyDescriptor<any>): void {
    metaContainer.addActionDecoratorMeta({
        name: "AllowAnonymous",
        type: "authentication",
        controllerPrototype: target,
        action: propertyKey,
        paramTypes: Reflect.getMetadata("design:paramtypes", target, propertyKey),
        data: undefined
    });
}
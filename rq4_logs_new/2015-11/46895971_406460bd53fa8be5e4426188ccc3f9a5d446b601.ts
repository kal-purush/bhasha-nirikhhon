import option = require("./monad/option");
export var Option = option.Option;

import _try = require("./monad/try");
export var Try = _try.Try;

import callback = require("./util/callback");
export var Callback = callback.Callback;


export function isString(obj: any): boolean {
    if (!obj) {
        return (obj === "");
    }
    return typeof(obj) === "string" || obj instanceof String;
}


export var isArray: (obj: any) => boolean = Array.isArray;


export function isNumber(obj: any): boolean {
    if (!obj) {
        return (obj === 0);
    }
    return (typeof(obj) === "number" || obj instanceof Number);
}


export function isFunction(obj: any): boolean {
    return obj && (typeof(obj) === "function" || obj instanceof Function);
}


// isPlainObject にすべき？
export function isObject(obj: any): boolean {
    return Object.prototype.toString.call(obj) === "[object Object]";
}


export function isEmptyObject(obj: any): boolean {
    if (isObject(obj)) {
        var name;
        for ( name in obj ) {
            return false;
        }
        return true;
    }
    return false;
}


export function isNone(obj: any): boolean {
    return (obj === undefined) || (obj === null);
}


export function isBoolean(obj: any): boolean {
    return (obj === true) || (obj === false);
}


function equalArray(obj1: any[], obj2: any[]): boolean {
    var len: number = obj1.length;
    var i: number;

    if (len !== obj2.length) {
        return false;
    }

    for (i = 0; i < len; i++) {
        if (obj1[i] !== obj2[i]) {
            return false;
        }
    }
    return true;
}


export function isEqualObject(obj1: any, obj2: any): boolean {
    var i: number;
    var len: number;

    if (obj1 === obj2) {
        return true;
    }

    if (isObject(obj1) && isObject(obj2)) {
        var keys1: any[] = Object.keys(obj1);
        var keys2: any[] = Object.keys(obj2);
        if (equalArray(keys1, keys2)) {
            len = keys1.length;
            for (i = 0; i < len; i++) {
                if (obj1[keys1[i]] !== obj2[keys2[i]]) {
                    return false;
                }
            }
            return true;
        }
    }
    if (isArray(obj1) && isArray(obj2)) {
        return equalArray(obj1, obj2);
    }
    return false;
}


export function isEqualArray(obj1: any, obj2: any): boolean {
    var len: number;
    var i: number;

    if (isArray(obj1) && isArray(obj2)) {
        return equalArray(obj1, obj2);
    }
    return false;
}
// Type definitions for chai 2.0.0
// Project: http://chaijs.com/
// Definitions by: Dean Brettle <https://github.com/brettle/>
// Definitions: https://github.com/borisyankov/DefinitelyTyped

/// <reference path="chai.d.ts" />

declare module chai {
    export function should(): Should;

    export interface Should extends ShouldNot {
        not: ShouldNot
    }
    
    export interface ShouldNot {
        equal(act: any, exp: any, msg?: string):void;
        Throw(fn: Function, msg?: string):void;
        Throw(fn: Function, regExp: RegExp):void;
        Throw(fn: Function, errType: Function, msg?: string):void;
        Throw(fn: Function, errType: Function, regExp: RegExp):void;
        exist(val: any, msg?: string):void;
    }
}

interface Object {
    should: chai.Expect;
}

declare module "chai-should" {
    export = chai;
}
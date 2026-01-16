/// <reference path="../../typings/tsd.d.ts" />
export function IsNotNullnorUndefined(obj: any): boolean {
    return typeof obj !== 'undefined' && null !== obj;
}

export function IsNullOrUndefined(obj: any): boolean {
    return typeof obj == 'undefined' || null == obj;
}

export function GetObjectFromName(name: string, modulePath?: string): any {
    var ctor = null;
    if (IsNotNullnorUndefined(modulePath) && modulePath.length > 0) {
        ctor = require(modulePath);
    }
    try {
        return getObject(name, ctor);
    } catch (error) {
        throw new Error(`resolve module=${modulePath} name=${name} error, reason=` + error.toStirng());
    }
}

function getObject(stringNotation: string, ctor: any = null): any {

    let arrayNotation = stringNotation.split('.');
    let propObj = null;
    if (IsNullOrUndefined(ctor)) {
        propObj = global[arrayNotation[0]];
    }
    else {
        let rootObj = ctor;
        if (arrayNotation.length <= 1) {
            return rootObj;
        }
        propObj = rootObj[arrayNotation[1]];
        arrayNotation.shift();
    }

    if (IsNotNullnorUndefined(propObj)) {
        for (let i = 1; i < arrayNotation.length; i++) {
            let nextProp = propObj[arrayNotation[i]];
            if (IsNotNullnorUndefined(nextProp)) {
                propObj = nextProp;
            } else {
                throw new Error(`There's no ${arrayNotation[i]} property!`);
            }
        }
    }
    return propObj;
}
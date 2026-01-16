export function once(fn: Function): Function {
    let returnValue: any;
    let called = false;
    return function() {
        if (!called) {
            called = true;
            returnValue = fn.apply(this, arguments);
        }
        return returnValue;
    };
}

export function sum(a: number, b: number): number {
    return a + b;
}
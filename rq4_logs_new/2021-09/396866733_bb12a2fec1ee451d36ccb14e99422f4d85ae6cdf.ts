/**
 * This function takes an API entry point for a typical chaining-style
 * assertion library and wraps it so errors are passed to a callback
 * instead of being thrown.
 * 
 * @param {T} subject An API entry point for an assertion library, e.g. 'strict'
 * in the node.js assert module, or 'expect' in chai.
 * 
 * @param {(error: any) => void} consume A callback to which to pass trapped errors.
 */
export function soften<T extends object>(subject: T, consume: (error: any) => void): T {
    return wrap(subject, callback => {
        try {
            return callback();
        } catch (e) {
            consume(e);
            return null;
        }
    });
}
export function silence<T extends object>(subject: T): T {
    return wrap(subject, callback => {
        const log = console.log;
        try {
            console.log = () => {};
            return callback();
        } finally {
            console.log = log;
        }
    });
}
export function wrap<T extends object>(subject: T, wrapper: <U>(callback: () => U) => U | null): T {
    return new Proxy<T>(subject, {
        get: function (target, prop, receiver) {
            if (prop === subjectSymbol)
                return subject;
            return wrapper(() => wrap(Reflect.get(target, prop, receiver), wrapper));
        },
        apply: function (target, thisArg, argumentsList) {
            return wrapper(() => {
                thisArg = thisArg?.[subjectSymbol] ?? thisArg;
                return wrap((target as Function).call(thisArg, ...argumentsList), wrapper);
            });
        }
    });
}
const subjectSymbol = Symbol('proxy-subject');
export type Unwait<T> = T extends (...args: infer A) => infer R
    ? Promise<T> & ((...args: A) => Unwait<R>)
    : T extends Promise<infer R>
    ? Promise<R> &
          {
              readonly [K in Exclude<keyof R, keyof Promise<R>>]: Unwait<R[K]>;
          }
    : Promise<T> &
          {
              readonly [K in Exclude<keyof T, keyof Promise<T>>]: Unwait<T[K]>;
          };

// tslint:disable-next-line: no-empty
const EMPTY_FN = () => {};
const IS_UNWAIT_PROXY_SYMBOL = Symbol.for("IS_UNWAIT_PROXY_SYMBOL");
const UNWAIT_PROMISE_VALUE = Symbol.for("UNWAIT_PROMISE_VALUE");
const cache = new WeakMap<any, any>();

function accessProperty(obj: any, propertyName: string | number | symbol) {
    if (!obj) {
        throw new TypeError(
            `Cannot access property ${propertyName.toString()} of ${obj}`,
        );
    }
    return obj[propertyName];
}

function apply(target: any, thisArg: any, argArray: any[]) {
    if (!target || typeof target !== "function") {
        throw new TypeError(`${target} is not a function`);
    }
    return target.apply(thisArg, argArray);
}

function construct(Target: any, argArray: any[]) {
    if (!Target || !Target.prototype) {
        throw new TypeError(`${Target} is not a constructor`);
    }
    return new Target(...argArray);
}

/**
 * Helper to wrap a promise and expose it's value synchronously when resolved
 */
function wrapPromise<T>(
    value: Promise<T> | T,
): Promise<T> & { [UNWAIT_PROMISE_VALUE]?: T } {
    const promise = Promise.resolve(value);
    promise.then((res) => {
        (promise as any)[UNWAIT_PROMISE_VALUE] = res;
    });
    return promise;
}

/**
 * Internal unwait implementation
 * @param target the target to proxy
 * @param parent Provides the correct `this` context to wrapped child methods
 */
function _unwait<T>(target: T, parent?: Promise<any>): Unwait<T> {
    if (cache.has(target as any)) {
        return cache.get(target);
    }
    let promise = wrapPromise(target);
    const asyncProxyHandler: ProxyHandler<any> = {
        construct(_, argArray: any[]) {
            return _unwait(promise.then((res) => construct(res, argArray)));
        },
        get(_, key) {
            // provide a way to determine if a value is an unwait proxy
            if (key === IS_UNWAIT_PROXY_SYMBOL) {
                return true;
            }
            // promise methods and properties should remain unchanged
            if (key in promise) {
                const property = (promise as any)[key];
                return typeof property === "function"
                    ? property.bind(promise)
                    : property;
            }
            // prefer using resolved value to allow cache lookup
            return promise[UNWAIT_PROMISE_VALUE]
                ? _unwait(
                      accessProperty(promise[UNWAIT_PROMISE_VALUE], key),
                      promise,
                  )
                : _unwait(
                      promise.then((res) => accessProperty(res, key)),
                      promise,
                  );
        },
        apply(_, thisArg: any, argArray: any[]) {
            if (thisArg && thisArg[IS_UNWAIT_PROXY_SYMBOL]) {
                thisArg = undefined;
            }
            let result: any;
            // patch the thisArg to be the parent unless specified
            if (parent && !thisArg) {
                // prefer using resolved value to allow cache lookup
                result = promise[UNWAIT_PROMISE_VALUE]
                    ? parent.then((parentThis) =>
                          apply(
                              promise[UNWAIT_PROMISE_VALUE],
                              parentThis,
                              argArray,
                          ),
                      )
                    : Promise.all([promise, parent]).then(([res, parentThis]) =>
                          apply(res, parentThis, argArray),
                      );
            } else {
                // prefer using resolved value to allow cache lookup
                result = promise[UNWAIT_PROMISE_VALUE]
                    ? apply(promise[UNWAIT_PROMISE_VALUE], thisArg, argArray)
                    : promise.then((res) => apply(res, thisArg, argArray));
            }
            return _unwait(result, promise);
        },
    };
    const proxy = new Proxy(EMPTY_FN, asyncProxyHandler);
    promise = wrapPromise(
        promise.then((res) => {
            if (typeof res === "object" || typeof res === "function") {
                cache.set(res, proxy);
            }
            return res;
        }),
    );
    return proxy;
}

/**
 * Creates a proxy that defers all property and function access until it's target result has resolved.
 *
 * The target, and any of it's properties and method results can be called immediately,
 * and will return a proxied promise of the result when all promises in the chain resolve.
 * @param target The target to proxy.
 */
export function unwait<T>(target: T) {
    return _unwait(target);
}

export default unwait;
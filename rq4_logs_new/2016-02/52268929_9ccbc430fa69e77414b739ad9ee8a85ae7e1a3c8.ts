export type Dict<V> = {[key: string]: V};

export type ValueCallback<V, R> = (value?: V, key?: string, dict?: Dict<V>) => R;

export type KeyCallback<V, R> = (key?: string, value?: V, dict?: Dict<V>) => R;

export type Entry<V> = [string, V];


// API.

export function count(dict: Dict<Object>): number {
    let n = 0;
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            n += 1;
        }
    }
    return n;
}

export function entries<V>(dict: Dict<V>): Array<Entry<V>> {
    let result: Array<Entry<V>> = [];
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            result.push([key, dict[key]]);
        }
    }
    return result;
}

export function every<V>(dict: Dict<V>, predicate: ValueCallback<V, boolean>, context?: Object): boolean {
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key) && !predicate.call(context, dict[key], key, dict)) {
            return false;
        }
    }
    return true;
};

export function filter<V>(dict: Dict<V>, predicate: ValueCallback<V, boolean>, context?: Object): Dict<V> {
    const result: Dict<V> = {};
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key) && predicate.call(context, dict[key], key, dict)) {
            result[key] = dict[key];
        }
    }
    return result;
}

export function forEach<V>(dict: Dict<V>, sideEffect: ValueCallback<V, void>, context?: Object): void {
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            sideEffect.call(context, dict[key], key, dict);
        }
    }
}

export function from<V>(entries: Array<Entry<V>>): Dict<V> {
    const result: Dict<V> = {};
    for (const [key, value] of entries) {
        result[key] = value;
    }
    return result;
}

export function get<V>(dict: Dict<V>, key: string, defaultValue?: V): V {
    if (Object.prototype.propertyIsEnumerable.call(dict, key)) {
        return dict[key];
    }
    return defaultValue;
}

export function has(dict: Dict<Object>, key: string): boolean {
    return Object.prototype.propertyIsEnumerable.call(dict, key);
}

export function isEmpty(dict: Dict<Object>): boolean {
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            return false;
        }
    }
    return true;
}

export const keys = Object.keys;

export function map<V, R>(dict: Dict<V>, mapper: ValueCallback<V, R>, context?: Object): Array<R> {
    const result: Array<R> = [];
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            result.push(mapper.call(context, dict[key], key, dict));
        }
    }
    return result;
};

export function mapEntries<V, R>(dict: Dict<V>, mapper: ValueCallback<V, Entry<R>>, context?: Object): Dict<R> {
    const result: Dict<R> = {};
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            const [newKey, newValue]: Entry<R> = mapper.call(context, dict[key], key, dict);
            result[newKey] = newValue;
        }
    }
    return result;
};

export function mapKeys<V>(dict: Dict<V>, mapper: KeyCallback<V, string>, context?: Object): Dict<V> {
    const result: Dict<V> = {};
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            result[mapper.call(context, key, dict[key], dict)] = dict[key];
        }
    }
    return result;
};

export function mapValues<V, R>(dict: Dict<V>, mapper: ValueCallback<V, R>, context?: Object): Dict<R> {
    const result: Dict<R> = {};
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            result[key] = mapper.call(context, dict[key], key, dict);
        }
    }
    return result;
};

export function reduce<V>(dict: Dict<V>, reducer: (reduction?: V, value?: V, key?: string, dict?: Dict<V>) => V): V;
export function reduce<V, R>(dict: Dict<V>, reducer: (reduction?: R, value?: V, key?: string, dict?: Dict<V>) => R, initialReduction: R, context?: Object): R;
export function reduce<V, R>(dict: Dict<V>, reducer: (reduction?: R, value?: V, key?: string, dict?: Dict<V>) => R, initialReduction?: Object, context?: Object): Object {
    let result = initialReduction;
    if (initialReduction === undefined) {
        let first = true;
        for (const key in dict) {
            if (Object.prototype.hasOwnProperty.call(dict, key)) {
                if (first) {
                    result = dict[key];
                    first = false;
                } else {
                    result = reducer.call(context, result, dict[key], key, dict);
                }
            }
        }
    } else {
        for (const key in dict) {
            if (Object.prototype.hasOwnProperty.call(dict, key)) {
                result = reducer.call(context, result, dict[key], key, dict);
            }
        }
    }
    return result;
}

export function set<V>(dict: Dict<V>, key: string, value: V): Dict<V> {
    const result: Dict<V> = Object.assign({}, dict);
    result[key] = value;
    return result;
}

export function some<V>(dict: Dict<V>, predicate: ValueCallback<V, boolean>, context?: Object): boolean {
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key) && predicate.call(context, dict[key], key, dict)) {
            return true;
        }
    }
    return false;
};

export function values<V>(dict: Dict<V>): Array<V> {
    let result: Array<V> = [];
    for (const key in dict) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) {
            result.push(dict[key]);
        }
    }
    return result;
}
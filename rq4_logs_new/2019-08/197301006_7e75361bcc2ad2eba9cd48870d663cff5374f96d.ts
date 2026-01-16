import isPlainObject from 'is-plain-obj';

import { getAllProps } from './';

export interface StateInterface<T> {
    restore(): T;
}

class UnknownState<T> implements StateInterface<T> {
    private _ref: T;

    public constructor(value: T) {
        this._ref = value;
    }

    public restore(): T {
        return this._ref;
    }
}

class PlainObjectState<T extends object> extends UnknownState<T> {
    private _propMap: Map<keyof T, StateInterface<T[keyof T]>>;

    public constructor(origObj: T, props: readonly (keyof T)[] = []) {
        super(origObj);

        this._propMap = new Map();
        [...getAllProps(origObj), ...props].forEach(prop => {
            if (Object.prototype.hasOwnProperty.call(origObj, prop)) {
                // eslint-disable-next-line @typescript-eslint/no-use-before-define
                this._propMap.set(prop, createState(origObj[prop]));
            }
        });
    }

    public restore(): T {
        const origObj = super.restore();

        getAllProps(origObj).forEach(propName => {
            if (!this._propMap.has(propName)) {
                delete origObj[propName];
            }
        });

        this._propMap.forEach((valueState, propName) => {
            origObj[propName] = valueState.restore();
        });

        return origObj;
    }
}

class RegExpState<T extends RegExp> extends PlainObjectState<T> {
    public constructor(origRegExp: T) {
        super(origRegExp, ['lastIndex']);
    }
}

class DateState<T extends Date> extends PlainObjectState<T> {
    private _unixtime: number;

    public constructor(origDate: T) {
        super(origDate);
        this._unixtime = origDate.getTime();
    }

    public restore(): T {
        const origDate = super.restore();
        origDate.setTime(this._unixtime);
        return origDate;
    }
}

class BufferState<T extends Buffer> extends PlainObjectState<T> {}

class MapState<
    T extends Map<K, V>,
    K = unknown,
    V = unknown
> extends PlainObjectState<T> {
    private _entries: readonly (readonly [K, V])[];

    public constructor(origMap: T) {
        super(origMap);
        this._entries = [...origMap.entries()];
    }

    public restore(): T {
        const origMap = super.restore();

        origMap.clear();

        this._entries.forEach(([key, value]) => {
            origMap.set(key, value);
        });

        return origMap;
    }
}

class SetState<T extends Set<V>, V = unknown> extends PlainObjectState<T> {
    private _values: readonly V[];

    public constructor(origSet: T) {
        super(origSet);
        this._values = [...origSet.values()];
    }

    public restore(): T {
        const origSet = super.restore();

        origSet.clear();

        this._values.forEach(value => {
            origSet.add(value);
        });

        return origSet;
    }
}

class ErrorState<T extends Error> extends PlainObjectState<T> {
    public constructor(origError: T) {
        super(origError, ['name', 'message', 'stack']);
    }
}

/**
 * @see https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Structured_clone_algorithm
 */
export default function createState<T>(value: T): StateInterface<T> {
    if (
        isPlainObject(value) ||
        Array.isArray(value) ||
        value instanceof Boolean ||
        value instanceof String
    ) {
        return new PlainObjectState<typeof value>(value);
    }
    if (value instanceof RegExp) {
        return new RegExpState<typeof value>(value);
    }
    if (value instanceof Date) {
        return new DateState<typeof value>(value);
    }
    // TODO: TypedArray support
    if (Buffer.isBuffer(value)) {
        return new BufferState<typeof value>(value);
    }
    if (value instanceof Map) {
        return new MapState<typeof value>(value);
    }
    if (value instanceof Set) {
        return new SetState<typeof value>(value);
    }
    if (value instanceof Error) {
        return new ErrorState<typeof value>(value);
    }
    return new UnknownState(value);
}
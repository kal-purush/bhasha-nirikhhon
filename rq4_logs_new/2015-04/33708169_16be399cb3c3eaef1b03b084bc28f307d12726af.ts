/// <reference path="../typings/knockout/knockout.d.ts" />
/// <reference path="../typings/underscore/underscore.d.ts" />

interface UnderscoreStatic {
    sum<T>(list: _.List<T>, iterator: _.ListIterator<T, number>, context?: any): number;
    sum<T>(object: _.Dictionary<T>, iterator: _.ObjectIterator<T, number>, context?: any): number;

    average<T>(list: _.List<T>, iterator: _.ListIterator<T, number>, context?: any): number;
    average<T>(object: _.Dictionary<T>, iterator: _.ObjectIterator<T, number>, context?: any): number;

    count<T>(list: _.List<T>, iterator?: _.ListIterator<T, boolean>, context?: any): number;
    count<T>(object: _.Dictionary<T>, iterator?: _.ObjectIterator<T, boolean>, context?: any): number;

    filterMap<T, TResult>(list: _.List<T>, iterator: _.ListIterator<T, TResult>, context?: any): TResult[];
    filterMap<T, TResult>(object: _.Dictionary<T>, iterator: _.ObjectIterator<T, TResult>, context?: any): TResult[];

    index<T>(list: _.List<T>, iterator: _.ListIterator<T, boolean>, context?: any): number;
    index<T>(object: _.Dictionary<T>, iterator: _.ObjectIterator<T, boolean>, context?: any): number;

    partialEnd(func: Function, ...args: any[]): Function;
}

interface KnockoutUnderscoreArrayFunctions<T> {
    each(iterator: _.ListIterator<T, void>, context?: any): void;
    map<TResult>(iterator: _.ListIterator<T, TResult>, context?: any): TResult[];
    filterMap<TResult>(iterator?: _.ListIterator<T, TResult>, context?: any): TResult[];
    reduce<TResult>(iterator: _.MemoIterator<T, TResult>, memo: TResult, context?: any): TResult;
    find(iterator: _.ListIterator<T, boolean>, context?: any): T;
    filter(iterator: _.ListIterator<T, boolean>, context?: any): T[];
    reject(iterator: _.ListIterator<T, boolean>, context?: any): T[];
    sum(iterator: _.ListIterator<T, number>, context?: any): number;
    average(iterator: _.ListIterator<T, number>, context?: any): number;
    all(iterator: _.ListIterator<T, boolean>, context?: any): boolean;
    any(iterator: _.ListIterator<T, boolean>, context?: any): boolean;
    contains(value: T): boolean;
    max(iterator: _.ListIterator<T, number>, context?: any): T;
    min(iterator: _.ListIterator<T, number>, context?: any): T;
    sortBy<TSort>(iterator: _.ListIterator<T, TSort>, context?: any): T[];
    groupBy(iterator: _.ListIterator<T, any>, context?: any): { [key: string]: any[]; };
    toArray(): any[];
    count(iterator: _.ListIterator<T, boolean>, context?: any): number;
    index(iterator?: _.ListIterator<T, boolean>, context?: any): number;
    size(): number;
    initial(n?: number): T[];
    rest(index?: number): T[];
    compact(): T[];
    flatten(shallow?: boolean): any;
    without(...values: T[]): T[];
    union(...arrays: T[][]): T[];
    intersection(...arrays: T[][]): T[];
    difference(...others: T[][]): T[];
    uniq<TSort>(isSorted?: boolean, iterator?: _.ListIterator<T, TSort>, context?: any): T[];
    zip(...arrays: any[][]): any[][];

    first(): T;
    last(): T;
    indexOf(value: T, isSorted?: boolean): number;
    lastIndexOf(value: T, from?: number): number;

    _map<TResult>(iterator: _.ListIterator<T, TResult>, context?: any): KnockoutUnderscoreComputed<TResult>;
    _filterMap<TResult>(iterator?: _.ListIterator<T, TResult>, context?: any): KnockoutUnderscoreComputed<TResult>;
    _filter(iterator: _.ListIterator<T, boolean>, context?: any): KnockoutUnderscoreComputed<T>;
    _reject(iterator: _.ListIterator<T, boolean>, context?: any): KnockoutUnderscoreComputed<T>;
    _sortBy<T, TSort>(iterator: _.ListIterator<T, TSort>, context?: any): KnockoutUnderscoreComputed<T>;
    _toArray(): KnockoutUnderscoreComputed<T>;

    _each(iterator: _.ListIterator<T, void>, context?: any): KnockoutComputed<void>;
    _reduce<TResult>(iterator: _.MemoIterator<T, TResult>, memo: TResult, context?: any): KnockoutComputed<TResult>;
    _find(iterator: _.ListIterator<T, boolean>, context?: any): KnockoutComputed<T>;
    _sum(iterator: _.ListIterator<T, number>, context?: any): KnockoutComputed<number>;
    _average(iterator: _.ListIterator<T, number>, context?: any): KnockoutComputed<number>;
    _all(iterator: _.ListIterator<T, boolean>, context?: any): KnockoutComputed<boolean>;
    _any(iterator: _.ListIterator<T, boolean>, context?: any): KnockoutComputed<boolean>;
    _contains(value: T): KnockoutComputed<boolean>;
    _min(iterator: _.ListIterator<T, number>, context?: any): KnockoutComputed<T>;
    _max(iterator: _.ListIterator<T, number>, context?: any): KnockoutComputed<T>;
    _groupBy(iterator: (element: T, index?: number, list?: T[]) => string, context?: any): KnockoutComputed<{ [key: string]: any[]; }>;
    _count(iterator: _.ListIterator<T, boolean>, context?: any): KnockoutComputed<number>;
    _index(iterator?: _.ListIterator<T, boolean>, context?: any): KnockoutComputed<number>;
    _size(): KnockoutComputed<number>;

    _initial(n?: number): KnockoutUnderscoreComputed<T>;
    _rest(index?: number): KnockoutUnderscoreComputed<T>;
    _compact(): KnockoutUnderscoreComputed<T>;
    _flatten(shallow?: boolean): KnockoutUnderscoreComputed<any>;
    _without(...values: T[]): KnockoutUnderscoreComputed<T>;
    _union(...arrays: T[][]): KnockoutUnderscoreComputed<T>;
    _intersection(...arrays: T[][]): KnockoutUnderscoreComputed<T>;
    _difference(...others: T[][]): KnockoutUnderscoreComputed<T>;
    _uniq<TSort>(isSorted?: boolean, iterator?: _.ListIterator<T, TSort>, context?: any): KnockoutUnderscoreComputed<T>;
    _zip(...arrays: any[][]): KnockoutUnderscoreComputed<any[]>;

    _first(): KnockoutComputed<T>;
    _last(): KnockoutComputed<T>;
    _indexOf(value: T, isSorted?: boolean): KnockoutComputed<number>;
    _lastIndexOf(value: T, from?: number): KnockoutComputed<number>;
}

interface KnockoutUnderscoreObjectsFunctions<T> {
    keys(): string[];
    values(): any[];
    clone(object: T): T;
    isEmpty(object: any): boolean;

    _keys(): KnockoutUnderscoreComputed<string>;
    _values(): KnockoutUnderscoreComputed<any>;
    _clone(object: T): KnockoutComputed<T>;
    _isEmpty(object: any): KnockoutComputed<boolean>;
}

interface KnockoutObservableArrayFunctions<T> extends KnockoutUnderscoreArrayFunctions<T> {
    indexOf(value: T, isSorted?: boolean): number;
    lastIndexOf(value: T, from?: number): number;
}

interface KnockoutUnderscoreComputed<T> extends KnockoutComputed<T[]> { }
interface KnockoutUnderscoreComputed<T> extends KnockoutUnderscoreArrayFunctions<T> { }

interface KnockoutExtenders {
    underscore(target: any, mode: string): any;
}

interface KnockoutUnderscore {
    objects: { [key: string]: Function };
    collections: { [key: string]: Function };

    addTo(val: any, mode?: string): void;
}

declare module "kounderscore" {
    var result: KnockoutUnderscore;
    export = result;
}

interface Window {
    ko_: KnockoutUnderscore;
}
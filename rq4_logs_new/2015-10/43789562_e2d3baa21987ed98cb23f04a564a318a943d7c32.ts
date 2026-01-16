/// <reference path="../typings/tsd.d.ts" />
import * as Q from 'q';
export interface LoaderFunction<T> {
    (id: string): Q.Promise<T>;
}
export interface CacheItem<T> {
    val: T;
    ttl: number;
}
export interface CacheDict<T> {
    [id: string]: CacheItem<T>;
}
export interface AsyncStorageEngine {
    setItem(id: string, val: any): any;
    getItem(id: string): any;
}
export declare class Cache<T> {
    loader: LoaderFunction<T>;
    memory: CacheDict<T>;
    timers: {
        [id: string]: number | NodeJS.Timer;
    };
    ttl: number;
    now: () => number;
    constructor(loader: LoaderFunction<T>);
    flush(): void;
    queueRemoval(id: string, ttl?: number): void;
    has(id: string): Boolean;
    remove(id: string): void;
    set(id: string, val: T): T;
    get(id: string): Q.Promise<T>;
    private deferredGet(id);
}
export declare class AsyncStorageCache<T> extends Cache<T> {
    storage: AsyncStorageEngine;
    key: string;
    constructor(loader: LoaderFunction<T>, storage: AsyncStorageEngine, key?: string);
    remove(id: string): void;
    set(id: string, val: T): T;
    read(): CacheDict<T>;
    write(): void;
}
export declare class LocalStorageCache<T> extends AsyncStorageCache<T> {
    constructor(loader: LoaderFunction<T>, key?: string);
}
module System {
    export module Collections {
        export module Generic {
            export interface List<T>
            {

                constructor( array: T[]);


                Aggregate: {
                    <TResult>(Func: (current: T, next: T) => TResult): T;
                    <TResult>(Func: (current: T, next: T) => TResult, seed: T ): T;
                };


                All: (Func: (x: T) => boolean) => boolean;
                Any: (Func: (x: T) => boolean) => boolean;

                Average: {
                    (): number;
                    (Func: (x: T) => number): number;
                };

                Concat: (array: T[]) => List<T>;

                Contains: {
                    (item: T): boolean;
                    (item: T, comparer: IEqualityComparer<T>): boolean;
                };

                Count: {
                    (): number;
                    (Func: (x: T) => boolean): number;
                };

                Distinct: {
                    (): List<T>;
                    (comparer: IEqualityComparer<T>): List<T>;
                };

                DistinctBy: {
                    <U>(Func: (x: T) => U): List<T>;
                    <U>(Func: (x: T) => U, comparer: IEqualityComparer<T>): List<T>
                };

                ElementAt: (index: number) => T;
                ElementAtOrDefault: (index: number) => T;

                Except: {
                    (except: T[]): List<T>;
                    (except: T[], comparer: IEqualityComparer<T>): List<T>;
                };

                First: {
                    (): T;
                    (Func: (x: T) => boolean): T;
                };

                FirstOrDefault: {
                    (): T;
                    (Func: (x: T) => boolean): T;
                };

                ForEach: (Action: (e: T, index: number) => any) => void;

                GroupBy: {
                    <TKey, TElement>(keySelector: (e: T) => TKey): List<any>;
                    <TKey, TElement>(keySelector: (e: T) => TKey, elementSelector: (e: T) => TElement, comparer: IEqualityComparer<TKey>): List<any>;
                    //<TKey, TElement>(keySelector: (e: T) => TKey, elementSelector: (e: T) => TElement): List<any>;
                }

                IndexOf: {
                    (e: T): number;
                    (e: T, comparer: IEqualityComparer<T>): number;
                    //(e: T, index: number): number;
                    //(e: T, index: number, count: number): number; // Need to implement it
                };

                Intersect: {
                    (array: T[]): List<T>;
                    (array: T[], comparer: IEqualityComparer<T>): List<T>;
                };

                Join: {
                    <TInner, TKey, TResult>(array: TInner[]
                        , outerKeySelector: (e: T) => TKey
                        , innerKeySelector: (e: TInner) => TKey
                        , resultSelector: (outer: T, inner: TInner) => TResult): List<TResult>;

                    <TInner, TKey, TResult>(array: TInner[]
                        , outerKeySelector: (e: T) => TKey
                        , innerKeySelector: (e: TInner) => TKey
                        , resultSelector: (outer: T, inner: TInner) => TResult
                        , comparer: IEqualityComparer<TKey>): List<TResult>
                }

                Last: {
                    (): T;
                    (Func: (x: T) => boolean): T;
                };

                LastOrDefault: {
                    (): T;
                    (Func: (x: T) => boolean): T;
                };

                Max: {
                    <TResult>(): TResult;
                    <TResult>(Func: (x: T) => TResult): TResult;
                };

                Min: {
                    <TResult>(): TResult;
                    <TResult>(Func: (x: T) => TResult): TResult;
                };

                OrderBy: {
                    <TKey>(Func: (x: T) => TKey): List<T>;
                    <TKey>(Func: (x: T) => TKey, comparer: (a: TKey, b: TKey) => number): List<T>;
                };

                OrderByDescending: {
                    <TKey>(Func: (x: T) => TKey): List<T>;
                    <TKey>(Func: (x: T) => TKey, comparer: (a: TKey, b: TKey) => number): List<T>;
                };

                Reverse: () => List<T>;

                Select: {
                    <TResult>(selector: (e: T) => TResult): List<TResult>;
                    <TResult>(selector: (e: T) => TResult, i: number): List<TResult>;
                };

                SelectMany: {
                    <TResult>(selector: (e: T) => T[]): List<TResult>;
                    <TResult>(selector: (e: T) => T[], resultSelector: (e: T) => TResult): List<TResult>;
                };

                SequenceEqual: {
                    (second: T[]): boolean;
                    (second: T[], comparer: (a: T, b: T) => boolean): boolean;
                };

                Single: {
                    (): T;
                    (Func: (x: T) => boolean): T;
                };

                SingleOrDefault: {
                    (): T;
                    (Func: (x: T) => boolean): T;
                };

                Skip: (count: number) => List<T>;
                SkipWhile: (Func: (x: T) => boolean) => List<T>;

                Sum: {
                    (): number;
                    (Func: (x: T) => number): number;
                };

                Take: (count: number) => List<T>;

                TakeWhile: (Func: (x: T) => boolean) => List<T>;

                Union: {
                    (array: T[]): List<T>;
                    (array: T[], comparer?: IEqualityComparer<T>): List<T>;
                };

                Where: (Func: (x: T) => boolean) => List<T>;

                Zip: { <TInner, TResult>(array: TInner[], resultSelector: (o: T, i: TInner) => TResult): List<TResult> };

                ToArray: () => T[];
            }
        }
    }
} 

import List = System.Collections.Generic.List;

interface Array<T> {
    ToList<T>(): List<T>;
}
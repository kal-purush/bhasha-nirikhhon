module Streamalicious.Log {
    export function log(message: string): void {
        //		console.log(message);
    }
}

module Streamalicious {

    export interface Streamable<T> {
        getPart(): T[];
    }

    interface Transformer<T, U> {
        (value: T): U;
    }

    class TransformingStreamable<T, U> implements Streamable<U> {
        private streamable: Streamable<T>;
        private transformer: Transformer<T, U>;

        constructor(streamable: Streamable<T>, transformer: Transformer<T, U>) {
            this.streamable = streamable;
            this.transformer = transformer;
        }

        getPart() {
            var part = this.streamable.getPart();
            Streamalicious.Log.log("Untransformed part: " + part);
            if (!part) {
                return null;
            }
            var transformedPart = part.map(this.transformer);
            Streamalicious.Log.log("Transformed part: " + transformedPart);
            return transformedPart;
        }
    }

    export interface Predicate<T> {
        (value: T): boolean;
    }
    
    class FilteringStreamable<T> implements Streamable<T> {
        private streamable: Streamable<T>;
        private filter: Predicate<T>;

        constructor(streamable: Streamable<T>, filter: Predicate<T>) {
            this.streamable = streamable;
            this.filter = filter;
        }

        getPart() {
            var part = this.streamable.getPart();
            Streamalicious.Log.log("Unfiltered part: " + part);
            if (!part) {
                return null;
            }
            var filteredPart = part.filter(this.filter);
            Streamalicious.Log.log("Filterd part: " + filteredPart);
            return filteredPart;
        }
    }

    class LimitingStreamable<T> implements Streamable<T> {
        private streamable: Streamable<T>;
        private lengthLeft: number;

        constructor(streamable: Streamable<T>, length: number) {
            this.streamable = streamable;
            this.lengthLeft = length;
        }

        getPart() {
            if (this.lengthLeft === 0) {
                return null;
            }
            var part = this.streamable.getPart();
            if (!part) {
                return null;
            }
            if (this.lengthLeft >= part.length) {
                this.lengthLeft -= part.length;
                return part;
            }
            var limitedPart = part.slice(0, this.lengthLeft);
            Streamalicious.Log.log("Limited part: " + limitedPart);
            this.lengthLeft = 0;
            return limitedPart;
        }
    }

    interface SplittingCache<T> {
        content: T[];
    }

    class SplittingStreamable<T> implements Streamable<T> {
        private streamable: Streamable<T>;
        private myCache: SplittingCache<T>;
        private otherCache: SplittingCache<T>;

        constructor(streamable: Streamable<T>, myCache: SplittingCache<T>, otherCache: SplittingCache<T>) {
            this.streamable = streamable;
            this.myCache = myCache;
            this.otherCache = otherCache;
        }

        getPart() {
            var part: T[];
            if (this.otherCache.content) {
                part = this.otherCache.content;
                this.otherCache.content = null;
                return part;
            }
            part = this.streamable.getPart();
            this.myCache.content = this.myCache.content ? this.myCache.content.concat(part) : part;
            return part;
        }
    }

    export interface Collector<T, U> {
        collect(streamable: Streamable<T>): U;
    }

    interface Transformer<T, U> {
        (value: T): U;
    }

    export class Stream<T> {
        private streamable: Streamable<T>;

        constructor(streamable: Streamable<T>) {
            this.streamable = streamable;
        }

        collect<U>(collector: Collector<T, U>) {
            return collector.collect(this.streamable);
        }

        transform<U>(transformer: Transformer<T, U>) {
            return new Stream(new TransformingStreamable(this.streamable, transformer));
        }

        filter(filter: Predicate<T>) {
            return new Stream(new FilteringStreamable(this.streamable, filter));
        }

        limit(length: number) {
            return new Stream(new LimitingStreamable(this.streamable, length));
        }

        split() {
            var leftCache: SplittingCache<T> = { content: null };
            var rightCache: SplittingCache<T> = { content: null };
            var left = new SplittingStreamable(this.streamable, leftCache, rightCache);
            var right = new SplittingStreamable(this.streamable, rightCache, leftCache);
            return [new Stream(left), new Stream(right)];
        }
    }
}

module Streamalicious.Utils {
    class ArrayStreamable<T> implements Streamable<T> {
        private array: T[];
        
        constructor(array: T[]) {
            this.array = array;
        }

        getPart() {
            var part = this.array;
            this.array = null;
            return part;
        }
    }

    class RangeStreamable implements Streamable<number> {
        private current: number;
        private last: number;
        
        constructor(first: number, last: number) {
            this.current = first;
            this.last = last;
        }

        getPart() {
            if (this.current > this.last) {
                return null;
            }
            var result = [this.current];
            this.current++;
            return result;
        }
    }

    interface StreamSource<T> {
        (): T[];
    }

    class SourceStreamable<T> implements Streamable<T> {
        private source: StreamSource<T>;

        constructor(source: StreamSource<T>) {
            this.source = source;
        }

        getPart() {
            return this.source();
        }
    }

    export class StreamBuilder {
        static fromArray<T>(array: T[]) {
            return new Stream(new ArrayStreamable(array));
        }

        static fromRange(first: number, last: number) {
            return new Stream(new RangeStreamable(first, last));
        }

        static fromSource<T>(source: StreamSource<T>) {
            return new Stream(new SourceStreamable(source));
        }
    }

    class StringCollector implements Collector<string, string> {
        private seperator: string;

        constructor(seperator: string) {
            if (seperator === void 0) { seperator = ""; }
            this.seperator = seperator;
        }

        collect(streamable: Streamable<string>) {
            var all: string[] = [];
            var part: string[];
            while (part = streamable.getPart()) {
                all = all.concat(part);
            }
            ;
            return all.join(this.seperator);
        }
    }


    class CountCollector<T> implements Collector<T, number> {
        collect(streamable: Streamable<T>) {
            var count = 0;
            var part: T[];
            while (part = streamable.getPart()) {
                count += part.length;
            }
            return count;
        }
    }

    class HasAnyCollector<T> implements Collector<T, boolean> {
        private predicate: Predicate<T>;

        constructor(predicate: Predicate<T>) {
            this.predicate = predicate;
        }

        collect(streamable: Streamable<T>) {
            var part: T[];
            while (part = streamable.getPart()) {
                if (part.some(this.predicate)) {
                    return true;
                }
            }

            return false;
        }
    }

    export class CollectorBuilder {
        static createStringJoining(seperator: string) {
            return new StringCollector(seperator);
        }
        static createCounting() {
            return new CountCollector();
        }

        static createHasAny<T>(predicate: Predicate<T>) {
            return new HasAnyCollector(predicate);
        }
    }
}

module TestBed {
    var sb = Streamalicious.Utils.StreamBuilder;
    var cb = Streamalicious.Utils.CollectorBuilder;

    var result = sb.fromArray([1, 2, 3, 4])
        .transform((item: number) => { return "" + item; })
        .collect(cb.createStringJoining(", "));
    console.log("Result from array: " + result);

    var result2 = sb.fromRange(0, 42)
        .transform((item: number) => { return "" + item; })
        .collect(cb.createStringJoining(", "));
    console.log("Result from range: " + result2);

    var result3 = sb.fromRange(1, 1000)
        .collect(cb.createCounting());
    console.log("Result from counting collector: " + result3);

    var result4 = sb.fromRange(1, 1000)
        .limit(42)
        .collect(cb.createCounting());
    console.log("Result from limited counting collector: " + result4);

    var result5 = sb.fromRange(1, 1000)
        .limit(5)
        .transform((item: number) => { return "" + item; })
        .collect(cb.createStringJoining(", "));
    console.log("First 5 values in range: " + result5);

    var streams = sb.fromSource(() => { return [Math.random()]; })
        .limit(5)
        .split();
    var result6 = streams[1]
        .transform((item: number) => { return "" + item; })
        .collect(cb.createStringJoining(", "));
    console.log("5 random values: " + result6);

    var result7 = streams[0]
        .transform(function(item: number) { return "" + item; })
        .collect(cb.createHasAny(function(element) { return element < 0.1; }));
    console.log("5 random values has any under 0.1: " + result7);
}
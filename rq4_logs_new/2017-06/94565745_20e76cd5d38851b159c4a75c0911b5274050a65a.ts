import * as fs from "fs";
import * as path from "path";

export interface JavascriptIteratorResult<T> {
    done: boolean;
    value: T;
}

export class JavascriptIterator<T> {
    private _hasInitialValue: boolean;
    private _visitedInitialValue: boolean = false;

    constructor(private _iterator: Iterator<T>) {
        this._hasInitialValue = _iterator.hasCurrent();
    }

    public next(): JavascriptIteratorResult<T> {
        let done: boolean;
        if (this._hasInitialValue && !this._visitedInitialValue) {
            this._visitedInitialValue = true;
            done = false;
        }
        else {
            done = !this._iterator.next();
        }

        return {
            done: done,
            value: this._iterator.getCurrent()
        };
    }
}

/**
 * An interface that iterates over a collection of values.
 */
export interface Iterator<T> {
    /**
     * The iterator function that gets called when this object is passed into a for-of loop.
     */
    [Symbol.iterator](): JavascriptIterator<T>

    /**
     * Whether or not this Iterator has stated iterating.
     */
    hasStarted(): boolean;

    /**
     * Whether or not this Iterator is currently pointing at a value or not. The Iterator could not
     * be pointing at a value if it hasn't started iterating, or if it has finished iterating.
     */
    hasCurrent(): boolean;

    /**
     * Move this Iterator to the next value in the collection. Return whether or not this Iterator
     * has a current value when it is finished moving.
     */
    next(): boolean;

    /**
     * Get the current value that this Iterator is pointing at, or get undefined if the Iterator
     * doesn't have a current value.
     */
    getCurrent(): T;

    /**
     * Get the current value and move this iterator to the next value in the collection.
     */
    takeCurrent(): T;

    /**
     * Get whether or not this Iterator contains any values that match the provided condition. If
     * the condition is not defined, then this function returns whether the collection contains any
     * values. This function may advance the iterator.
     */
    any(condition?: (value: T) => boolean): boolean;

    /**
     * Get the number of values that this Iterator can iterate. The Iterator will not have a current
     * value when this function completes.
     */
    getCount(): number;

    /**
     * Get the first value in this Iterator that matches the provided condition. If no condition
     * is provided, then the first value in the Iterator will be returned. If the Iterator is empty,
     * then undefined will be returned.
     */
    first(condition?: (value: T) => boolean): T;

    /**
     * Place each of the values of this Iterator into an array.
     */
    toArray(): T[];

    /**
     * Place each of the values of this Iterator into an ArrayList.
     */
    toArrayList(): ArrayList<T>;

    /**
     * Get an Iterator based on this Iterator that only returns values that match the provided
     * condition.
     */
    where(condition: (value: T) => boolean): Iterator<T>;

    /**
     * Get an Iterator based on this Iterator that skips the provided number of values.
     */
    skip(toSkip: number): Iterator<T>;

    /**
     * Get an Iterator based on this Iterator that only returns the provided number of values.
     */
    take(toTake: number): Iterator<T>;

    /**
     * Get an Iterator based on this Iterator that maps each of this Iterator's values to a
     * different value.
     */
    map<U>(mapFunction: (value: T) => U): Iterator<U>;

    /**
     * Return a new iterator that concatenates the contents of the provided iterator to the contents
     * of this iterator.
     */
    concatenate(iterator: Iterator<T> | T[]): Iterator<T>;
}

export abstract class IteratorBase<T> implements Iterator<T> {
    public abstract hasStarted(): boolean;
    public abstract hasCurrent(): boolean;
    public abstract next(): boolean;
    public abstract getCurrent(): T;

    [Symbol.iterator](): JavascriptIterator<T> {
        return new JavascriptIterator<T>(this);
    }

    public takeCurrent(): T {
        const result: T = this.getCurrent();
        this.next();
        return result;
    }

    public getCount(): number {
        let result: number = 0;

        if (this.hasCurrent()) {
            ++result;
        }

        while (this.next()) {
            ++result;
        }

        return result;
    }

    public any(condition?: (value: T) => boolean): boolean {
        let result: boolean;

        if (!condition) {
            result = this.hasCurrent() || this.next();
        }
        else {
            result = false;
            for (const value of this) {
                if (condition(value)) {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    public first(condition?: (value: T) => boolean): T {
        let result: T;
        if (!condition) {
            if (!this.hasStarted()) {
                this.next();
            }
            result = this.getCurrent();
        }
        else {
            result = this.where(condition).first();
        }
        return result;
    }

    public toArray(): T[] {
        const result: T[] = [];
        for (const value of this) {
            result.push(value);
        }
        return result;
    }

    public toArrayList(): ArrayList<T> {
        return new ArrayList(this.toArray());
    }

    public where(condition: (value: T) => boolean): Iterator<T> {
        return new WhereIterator(this, condition);
    }

    public skip(toSkip: number): Iterator<T> {
        return new SkipIterator(this, toSkip);
    }

    public take(toTake: number): Iterator<T> {
        return new TakeIterator(this, toTake);
    }

    public map<U>(mapFunction: (value: T) => U): Iterator<U> {
        return new MapIterator<U, T>(this, mapFunction);
    }

    public concatenate(toConcatenate: Iterator<T> | T[]): Iterator<T> {
        let result: Iterator<T>;
        if (!toConcatenate) {
            result = this;
        }
        else {
            if (toConcatenate instanceof Array) {
                toConcatenate = new ArrayList<T>(toConcatenate).iterate();
            }
            result = new ConcatenateIterator<T>(this, toConcatenate);
        }
        return result;
    }
}

abstract class IteratorDecorator<T> extends IteratorBase<T> {
    constructor(private _innerIterator: Iterator<T>) {
        super();
    }

    public hasStarted(): boolean {
        return this._innerIterator.hasStarted();
    }

    public hasCurrent(): boolean {
        return this._innerIterator.hasCurrent();
    }

    public next(): boolean {
        return this._innerIterator.next();
    }

    public getCurrent(): T {
        return this._innerIterator.getCurrent();
    }
}

/**
 * An Iterator that only returns values from the inner iterator that match its condition.
 */
class WhereIterator<T> extends IteratorDecorator<T> {
    constructor(innerIterator: Iterator<T>, private _condition: (value: T) => boolean) {
        super(innerIterator);

        if (this._condition) {
            while (this.hasCurrent() && !this._condition(this.getCurrent())) {
                super.next();
            }
        }
    }

    public next(): boolean {
        if (!this._condition) {
            super.next();
        }
        else {
            while (super.next() && !this._condition(this.getCurrent())) {
            }
        }

        return this.hasCurrent();
    }
}

/**
 * An Iterator that skips the first number of values from the provided inner iterator.
 */
class SkipIterator<T> extends IteratorDecorator<T> {
    private _skipped: number = 0;

    constructor(innerIterator: Iterator<T>, private _toSkip: number) {
        super(innerIterator);
    }

    private skipValues(): void {
        while (this._skipped < this._toSkip) {
            if (!super.next()) {
                this._skipped = this._toSkip;
            }
            else {
                ++this._skipped;
            }
        }
    }

    public hasCurrent(): boolean {
        if (super.hasCurrent()) {
            this.skipValues();
        }
        return super.hasCurrent();
    }

    public next(): boolean {
        this.skipValues();
        return super.next();
    }

    public getCurrent(): T {
        if (super.hasCurrent()) {
            this.skipValues();
        }
        return super.getCurrent();
    }
}

/**
 * An Iterator that only takes at most the first number of values from the provided inner iterator.
 */
class TakeIterator<T> extends IteratorDecorator<T> {
    private _taken: number;

    constructor(innerIterator: Iterator<T>, private _toTake: number) {
        super(innerIterator);

        this._taken = super.hasCurrent() ? 1 : 0;
    }

    private canTakeValue(): boolean {
        return isDefined(this._toTake) && this._taken <= this._toTake;
    }

    public hasCurrent(): boolean {
        return super.hasCurrent() && this.canTakeValue();
    }

    public next(): boolean {
        if (this.canTakeValue()) {
            ++this._taken;
            super.next();
        }
        return this.hasCurrent();
    }

    public getCurrent(): T {
        return this.hasCurrent() ? super.getCurrent() : undefined;
    }
}

class MapIterator<OuterT, InnerT> implements Iterator<OuterT> {
    private _started: boolean;

    constructor(private _innerIterator: Iterator<InnerT>, private _mapFunction: (value: InnerT) => OuterT) {
        this._started = _innerIterator.hasStarted();
    }

    [Symbol.iterator](): JavascriptIterator<OuterT> {
        return new JavascriptIterator<OuterT>(this);
    }

    public hasStarted(): boolean {
        return this._started;
    }

    public hasCurrent(): boolean {
        return this._mapFunction ? this._innerIterator.hasCurrent() : false;
    }

    public next(): boolean {
        this._started = true;
        return isDefined(this._mapFunction) && this._innerIterator.next();
    }

    public takeCurrent(): OuterT {
        const result: OuterT = this.getCurrent();
        this.next();
        return result;
    }

    public getCurrent(): OuterT {
        return this.hasCurrent() ? this._mapFunction(this._innerIterator.getCurrent()) : undefined;
    }

    public any(condition?: (value: OuterT) => boolean): boolean {
        let result: boolean;

        if (!condition) {
            result = this.hasCurrent() || this.next();
        }
        else {
            result = false;
            for (const value of this) {
                if (condition(value)) {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    public getCount(): number {
        this._started = true;
        return isDefined(this._mapFunction) ? this._innerIterator.getCount() : 0;
    }

    public first(condition?: (value: OuterT) => boolean): OuterT {
        return this.where(condition).first();
    }

    public toArray(): OuterT[] {
        const result: OuterT[] = [];
        for (const value of this) {
            result.push(value);
        }
        return result;
    }

    public toArrayList(): ArrayList<OuterT> {
        return new ArrayList(this.toArray());
    }

    public where(condition: (value: OuterT) => boolean): Iterator<OuterT> {
        return new WhereIterator(this, condition);
    }

    public skip(toSkip: number): Iterator<OuterT> {
        return new SkipIterator(this, toSkip);
    }

    public take(toTake: number): Iterator<OuterT> {
        return new TakeIterator(this, toTake);
    }

    public map<NewT>(mapFunction: (value: OuterT) => NewT): Iterator<NewT> {
        return new MapIterator(this, mapFunction);
    }

    public concatenate(toConcatenate: Iterator<OuterT>): Iterator<OuterT> {
        return new ConcatenateIterator<OuterT>(this, toConcatenate);
    }
}

class ConcatenateIterator<T> extends IteratorBase<T> {
    public constructor(private _first: Iterator<T>, private _second: Iterator<T>) {
        super();
    }

    public hasStarted(): boolean {
        return this._first.hasStarted();
    }

    public hasCurrent(): boolean {
        return this._first.hasCurrent() || this._second.hasCurrent();
    }

    public next(): boolean {
        return this._first.next() || this._second.next();
    }

    public getCurrent(): T {
        return this._first.hasCurrent() ? this._first.getCurrent() : this._second.getCurrent();
    }
}

/**
 * An interface of a collection that can have its contents iterated through.
 */
export interface Iterable<T> {
    [Symbol.iterator](): JavascriptIterator<T>;

    /**
     * Create an iterator for this collection.
     */
    iterate(): Iterator<T>;

    /**
     * Create an iterator for this collection that iterates the collection in reverse order.
     */
    iterateReverse(): Iterator<T>;

    /**
     * Get whether or not this collection contains any values that match the provided condition. If
     * the condition is not defined, then this function returns whether the collection contains any
     * values.
     */
    any(condition?: (value: T) => boolean): boolean;

    /**
     * Get the number of values that are contained in this collection.
     */
    getCount(): number;

    /**
     * Get the value in this collection at the provided index. If the provided index is not defined
     * or is outside of this Iterable's bounds, then undefined will be returned.
     */
    get(index: number): T;

    /**
     * Get the value in this collection at the provided index from the end of the collection. If the
     * provided index is not defined or is outside of this Iterable's bounds, then undefined will be
     * returned.
     */
    getLast(index: number): T;

    /**
     * Get whether or not this Iterable contians the provided value using the provided comparison
     * function. If no comparison function is provided, then a simple '===' comparison will be used.
     */
    contains(value: T, comparison?: (lhs: T, rhs: T) => boolean): boolean;

    /**
     * Get the first value in this collection that matches the provided condition. If no condition
     * is provided, then the first value in the collection will be returned. If the collection is
     * empty, then undefined will be returned.
     */
    first(condition?: (value: T) => boolean): T;

    /**
     * Get the last value in this collection. If the collection is empty, then undefined will be
     * returned.
     */
    last(condition?: (value: T) => boolean): T;

    /**
     * Get the values of this Iterable that match the provided condition.
     */
    where(condition: (value: T) => boolean): Iterable<T>;

    /**
     * Get an Iterable that skips the first toSkip number of values from this Iterable.
     */
    skip(toSkip: number): Iterable<T>;

    /**
     * Get an Iterable that skips the last toSkip number of values from this Iterable.
     */
    skipLast(toSkip: number): Iterable<T>;

    /**
     * Get the first toTake number of values from this Iterable<T>.
     */
    take(toTake: number): Iterable<T>;

    /**
     * Get the last toTake number of values from this Iterable<T>.
     */
    takeLast(toTake: number): Iterable<T>;

    /**
     * Get an Iterable based on this Iterable that maps each of this Iterable's values to a
     * different value.
     */
    map<U>(mapFunction: (value: T) => U): Iterable<U>;

    /**
     * Get an Iterable that concatenates the values of this Iterable with the values of the provided
     * Iterable or Array.
     */
    concatenate(toConcatenate: Iterable<T> | T[]): Iterable<T>;

    /**
     * Convert the values of this Iterable into an array.
     */
    toArray(): T[];

    /**
     * Get whether or not this Iterable<T> ends with the provided values.
     */
    endsWith(values: Iterable<T>): boolean;
}

/**
 * A base implementation of the Iterable<T> interface that classes can extend to make implementing
 * Iterable<T> easier.
 */
export abstract class IterableBase<T> implements Iterable<T> {
    [Symbol.iterator](): JavascriptIterator<T> {
        return new JavascriptIterator<T>(this.iterate());
    }

    public abstract iterate(): Iterator<T>;

    public abstract iterateReverse(): Iterator<T>;

    public any(condition?: (value: T) => boolean): boolean {
        return this.iterate().any(condition);
    }

    public getCount(): number {
        return this.iterate().getCount();
    }

    public get(index: number): T {
        let result: T;
        if (0 <= index) {
            const iterator: Iterator<T> = this.iterate();
            while (iterator.next() && 0 < index) {
                --index;
            }
            result = iterator.getCurrent();
        }
        return result;
    }

    public getLast(index: number): T {
        let result: T;
        if (0 <= index) {
            const iterator: Iterator<T> = this.iterateReverse();
            while (iterator.next() && 0 < index) {
                --index;
            }
            result = iterator.getCurrent();
        }
        return result;
    }

    public contains(value: T, comparison?: (iterableValue: T, value: T) => boolean): boolean {
        if (!comparison) {
            comparison = (iterableValue: T, value: T) => iterableValue === value;
        }

        return this.any((iterableValue: T) => comparison(iterableValue, value));
    }

    public first(condition?: (value: T) => boolean): T {
        return this.iterate().first(condition);
    }

    public last(condition?: (value: T) => boolean): T {
        return this.iterateReverse().first(condition);
    }

    public where(condition: (value: T) => boolean): Iterable<T> {
        return condition ? new WhereIterable<T>(this, condition) : this;
    }

    public skip(toSkip: number): Iterable<T> {
        return toSkip && 0 < toSkip ? new SkipIterable(this, toSkip) : this;
    }

    public skipLast(toSkip: number): Iterable<T> {
        return toSkip && 0 < toSkip ? this.take(this.getCount() - toSkip) : this;
    }

    public take(toTake: number): Iterable<T> {
        return toTake && 0 < toTake ? new TakeIterable(this, toTake) : new ArrayList<T>();
    }

    public takeLast(toTake: number): Iterable<T> {
        let result: Iterable<T>;
        if (!toTake || toTake < 0) {
            result = new ArrayList<T>();
        }
        else {
            const count: number = this.getCount();
            if (count <= toTake) {
                result = this;
            }
            else {
                result = this.skip(count - toTake);
            }
        }
        return result;
    }

    public map<U>(mapFunction: (value: T) => U): Iterable<U> {
        return mapFunction ? new MapIterable(this, mapFunction) : new ArrayList<U>();
    }

    public concatenate(toConcatenate: Iterable<T> | T[]): Iterable<T> {
        return toConcatenate ? new ConcatenateIterable(this, toConcatenate) : this;
    }

    public toArray(): T[] {
        return this.iterate().toArray();
    }

    public endsWith(values: Iterable<T>): boolean {
        let result: boolean;

        if (!values) {
            result = false;
        }
        else {
            const valuesCount: number = values.getCount();
            if (valuesCount === 0) {
                result = false;
            }
            else if (this.getCount() < valuesCount) {
                result = false;
            }
            else {
                result = true;

                const thisLastValuesIterator: Iterator<T> = this.takeLast(valuesCount).iterate();
                const valuesIterator: Iterator<T> = values.iterate();
                while (thisLastValuesIterator.next() === valuesIterator.next() && thisLastValuesIterator.hasCurrent()) {
                    if (thisLastValuesIterator.getCurrent() !== valuesIterator.getCurrent()) {
                        result = false;
                        break;
                    }
                }
            }
        }

        return result;
    }
}

class WhereIterable<T> extends IterableBase<T> {
    constructor(private _innerIterable: Iterable<T>, private _condition: (value: T) => boolean) {
        super();
    }

    public iterate(): Iterator<T> {
        return this._innerIterable.iterate().where(this._condition);
    }

    public iterateReverse(): Iterator<T> {
        return this._innerIterable.iterateReverse().where(this._condition);
    }
}

class SkipIterable<T> extends IterableBase<T> {
    constructor(private _innerIterable: Iterable<T>, private _toSkip: number) {
        super();
    }

    public iterate(): Iterator<T> {
        return this._innerIterable.iterate().skip(this._toSkip);
    }

    public iterateReverse(): Iterator<T> {
        return this._innerIterable.iterateReverse().take(this.getCount());
    }

    public getCount(): number {
        let result: number = this._innerIterable.getCount();
        if (result <= this._toSkip) {
            result = 0;
        }
        else {
            result -= this._toSkip;
        }
        return result;
    }

    public get(index: number): T {
        return this._innerIterable.get(index + this._toSkip);
    }
}

class TakeIterable<T> extends IterableBase<T> {
    constructor(private _innerIterable: Iterable<T>, private _toTake: number) {
        super();
    }

    public iterate(): Iterator<T> {
        return this._innerIterable.iterate().take(this._toTake);
    }

    public iterateReverse(): Iterator<T> {
        return this._innerIterable.iterateReverse().skip(this._innerIterable.getCount() - this._toTake);
    }

    public getCount(): number {
        let result: number = this._innerIterable.getCount();
        if (this._toTake < result) {
            result = this._toTake;
        }
        return result;
    }

    public get(index: number): T {
        return 0 <= index && index < this.getCount() ? this._innerIterable.get(index) : undefined;
    }
}

class MapIterable<OuterT, InnerT> implements Iterable<OuterT> {
    constructor(private _innerIterable: Iterable<InnerT>, private _mapFunction: (value: InnerT) => OuterT) {
    }

    [Symbol.iterator](): JavascriptIterator<OuterT> {
        return new JavascriptIterator<OuterT>(this.iterate());
    }

    public iterate(): Iterator<OuterT> {
        return this._innerIterable.iterate().map(this._mapFunction);
    }

    public iterateReverse(): Iterator<OuterT> {
        return this._innerIterable.iterateReverse().map(this._mapFunction);
    }

    public any(condition?: (value: OuterT) => boolean): boolean {
        return this.iterate().any(condition);
    }

    public getCount(): number {
        return this._innerIterable.getCount();
    }

    public get(index: number): OuterT {
        return this._mapFunction && isDefined(index) && 0 <= index && index < this.getCount() ? this._mapFunction(this._innerIterable.get(index)) : undefined;
    }

    public getLast(index: number): OuterT {
        return this._mapFunction && isDefined(index) && 0 <= index && index < this.getCount() ? this._mapFunction(this._innerIterable.getLast(index)) : undefined;
    }

    public contains(value: OuterT, comparison?: (lhs: OuterT, rhs: OuterT) => boolean): boolean {
        if (!comparison) {
            comparison = (lhs: OuterT, rhs: OuterT) => lhs === rhs;
        }

        return this.any((iterableValue: OuterT) => comparison(iterableValue, value));
    }

    public first(condition?: (value: OuterT) => boolean): OuterT {
        return this.iterate().first(condition);
    }

    public last(condition?: (value: OuterT) => boolean): OuterT {
        return this.iterateReverse().first(condition);
    }

    public where(condition: (value: OuterT) => boolean): Iterable<OuterT> {
        return condition ? new WhereIterable(this, condition) : this;
    }

    public skip(toSkip: number): Iterable<OuterT> {
        return toSkip && 0 < toSkip ? new SkipIterable(this, toSkip) : this;
    }

    public skipLast(toSkip: number): Iterable<OuterT> {
        return toSkip && 0 < toSkip ? this.take(this.getCount() - toSkip) : this;
    }

    public take(toTake: number): Iterable<OuterT> {
        return toTake && 0 < toTake ? new TakeIterable(this, toTake) : new ArrayList<OuterT>();
    }

    public takeLast(toTake: number): Iterable<OuterT> {
        let result: Iterable<OuterT>;
        if (!toTake || toTake < 0) {
            result = new ArrayList<OuterT>();
        }
        else {
            const count: number = this.getCount();
            if (count <= toTake) {
                result = this;
            }
            else {
                result = this.skip(count - toTake);
            }
        }
        return result;
    }

    public map<NewT>(mapFunction: (value: OuterT) => NewT): Iterable<NewT> {
        return mapFunction ? new MapIterable<NewT, OuterT>(this, mapFunction) : new ArrayList<NewT>();
    }

    public concatenate(toConcatenate: Iterable<OuterT> | OuterT[]): Iterable<OuterT> {
        return toConcatenate ? new ConcatenateIterable<OuterT>(this, toConcatenate) : this;
    }

    public toArray(): OuterT[] {
        return this.iterate().toArray();
    }

    public endsWith(values: Iterable<OuterT>): boolean {
        let result: boolean;

        if (!values) {
            result = false;
        }
        else {
            const valuesCount: number = values.getCount();
            if (valuesCount === 0) {
                result = false;
            }
            else if (this.getCount() < valuesCount) {
                result = false;
            }
            else {
                result = true;

                const thisLastValuesIterator: Iterator<OuterT> = this.takeLast(valuesCount).iterate();
                const valuesIterator: Iterator<OuterT> = values.iterate();
                while (thisLastValuesIterator.next() === valuesIterator.next() && thisLastValuesIterator.hasCurrent()) {
                    if (thisLastValuesIterator.getCurrent() !== valuesIterator.getCurrent()) {
                        result = false;
                        break;
                    }
                }
            }
        }

        return result;
    }
}

class ConcatenateIterable<T> extends IterableBase<T> {
    private _first: Iterable<T>;
    private _second: Iterable<T>;

    constructor(first: Iterable<T>, second: Iterable<T> | T[]) {
        super();
        this._first = first;
        this._second = new ArrayList<T>(second);
    }

    public iterate(): Iterator<T> {
        return this._first.iterate().concatenate(this._second.iterate());
    }

    public iterateReverse(): Iterator<T> {
        return this._second.iterateReverse().concatenate(this._first.iterateReverse());
    }
}

export abstract class ArrayListIterator<T> extends IteratorBase<T> {
    protected _currentIndex: number;

    constructor(protected _arrayList: ArrayList<T>) {
        super();
    }

    /**
     * Whether or not this ArrayListIterator is at the end of its iterating.
     */
    protected abstract atEnd(): boolean;

    public hasStarted(): boolean {
        return isDefined(this._currentIndex);
    }

    public hasCurrent(): boolean {
        return this.hasStarted() && !this.atEnd();
    }

    public abstract next(): boolean;

    public getCurrent(): T {
        return this._arrayList.get(this._currentIndex);
    }
}

class ArrayListForwardIterator<T> extends ArrayListIterator<T> {
    constructor(arrayList: ArrayList<T>) {
        super(arrayList);
    }

    protected atEnd(): boolean {
        return this._currentIndex === this._arrayList.getCount();
    }

    public next(): boolean {
        if (!this.hasStarted()) {
            this._currentIndex = 0;
        }
        else if (!this.atEnd()) {
            ++this._currentIndex;
        }
        return !this.atEnd();
    }
}

class ArrayListReverseIterator<T> extends ArrayListIterator<T> {
    constructor(arrayList: ArrayList<T>) {
        super(arrayList);
    }

    protected atEnd(): boolean {
        return this._currentIndex < 0;
    }

    public next(): boolean {
        if (!this.hasStarted()) {
            this._currentIndex = this._arrayList.getCount() - 1;
        }
        else if (!this.atEnd()) {
            --this._currentIndex;
        }
        return !this.atEnd();
    }
}

export class ArrayList<T> extends IterableBase<T> {
    private _data: T[] = [];
    private _count: number = 0;

    constructor(values?: T[] | Iterable<T>) {
        super();

        this.addAll(values);
    }

    public iterate(): ArrayListIterator<T> {
        return new ArrayListForwardIterator<T>(this);
    }

    public iterateReverse(): ArrayListIterator<T> {
        return new ArrayListReverseIterator<T>(this);
    }

    public get(index: number): T {
        let result: T;
        if (isDefined(index) && 0 <= index && index < this._count) {
            result = this._data[index];
        }
        return result;
    }

    /**
     * Set the ArrayList value at the provided index to be the provided value. If the index is not
     * defined or is outside of the Arraylist's bounds, then this function will do nothing.
     */
    public set(index: number, value: T): void {
        if (isDefined(index) && 0 <= index && index < this._count) {
            this._data[index] = value;
        }
    }

    /**
     * Set the last ArrayList value to be the provided value. If the ArrayList is empty, then this
     * function will do nothing.
     */
    public setLast(value: T): void {
        if (this.any()) {
            this._data[this._count - 1] = value;
        }
    }

    public any(condition?: (value: T) => boolean): boolean {
        return condition ? super.any(condition) : this._count > 0;
    }

    public getCount(): number {
        return this._count;
    }

    public add(value: T): void {
        if (this._count === this._data.length) {
            this._data.push(value);
        }
        else {
            this._data[this._count] = value;
        }
        this._count++;
    }

    public addAll(values: T[] | Iterable<T>): void {
        if (values) {
            for (const value of values) {
                this.add(value);
            }
        }
    }

    public indexOf(value: T, comparer?: (lhs: T, rhs: T) => boolean): number {
        let result: number;
        for (let i = 0; i < this._count; ++i) {
            if (comparer ? comparer(this._data[i], value) : this._data[i] === value) {
                result = i;
                break;
            }
        }
        return result;
    }

    public removeAt(index: number): T {
        let result: T;
        if (isDefined(index) && 0 <= index && index < this._count) {
            result = this._data[index];

            for (let i = index; i < this._count - 1; ++i) {
                this._data[i] = this._data[i + 1];
            }
            this._data[this._count - 1] = undefined;
            this._count--;
        }
        return result;
    }

    public remove(value: T, comparer?: (lhs: T, rhs: T) => boolean): T {
        let result: T;

        const removeIndex: number = this.indexOf(value, comparer);
        if (isDefined(removeIndex)) {
            result = this.removeAt(removeIndex);
        }

        return result;
    }

    public removeFirst(): T {
        return this.removeAt(0);
    }

    public removeLast(): T {
        return this.removeAt(this.getCount() - 1);
    }
}

export interface KeyValuePair<KeyType, ValueType> {
    key: KeyType;
    value: ValueType;
}

/**
 * A map/dictionary collection that associates a key to a value.
 */
export class Map<KeyType, ValueType> {
    private _pairs = new ArrayList<KeyValuePair<KeyType, ValueType>>();

    constructor(initialValues?: KeyValuePair<KeyType, ValueType>[] | Iterable<KeyValuePair<KeyType, ValueType>>) {
        this.addAll(initialValues);
    }

    /**
     * Get the number of entries in this map.
     */
    public getCount(): number {
        return this._pairs.getCount();
    }

    /**
     * Add the provide key value pair to the Map. If an entry already exists with the provided key,
     * the existing entry will be overwritten by the provided values.
     */
    public add(key: KeyType, value: ValueType): void {
        const pair: KeyValuePair<KeyType, ValueType> = {
            key: key,
            value: value
        };
        this._pairs.remove(pair, (lhs, rhs) => lhs.key === rhs.key);
        this._pairs.add(pair);
    }

    /**
     * Add each of the provided pairs to this Map. If any of the entries already exists with the
     * provided key, the existing entry will be overwritten by the provided value.
     */
    public addAll(keyValuePairs: KeyValuePair<KeyType, ValueType>[] | Iterable<KeyValuePair<KeyType, ValueType>>): void {
        if (keyValuePairs) {
            for (const keyValuePair of keyValuePairs) {
                this.add(keyValuePair.key, keyValuePair.value);
            }
        }
    }

    /**
     * Get whether or not the map contains the provided key.
     */
    public contains(key: KeyType): boolean {
        return this._pairs.any((pair) => pair.key === key);
    }

    /**
     * Get the value associated with the provided key. If the provided key is not found in the map,
     * then undefined will be returned.
     */
    public get(key: KeyType): ValueType {
        const pair: KeyValuePair<KeyType, ValueType> = this._pairs.first((pair) => pair.key === key);
        return pair ? pair.value : undefined;
    }
}

/**
 * A stack collection that can only add and remove elements from one end.
 */
export class Stack<T> {
    private _values = new ArrayList<T>();

    /**
     * Get whether or not this stack has any values.
     */
    public any(): boolean {
        return this._values.any();
    }

    /**
     * Get the number of values that are on the stack.
     */
    public getCount(): number {
        return this._values.getCount();
    }

    /**
     * Get whether this stack contains the provided value using the optional comparison. If the
     * comparison function is not provided, then === will be used.
     * @param value The value to search for in this stack.
     * @param comparison The optional comparison function to use to compare values.
     */
    public contains(value: T, comparison?: (lhs: T, rhs: T) => boolean): boolean {
        return this._values.contains(value, comparison);
    }

    /**
     * Add the provided value to the top of the stack.
     * @param value The value to add.
     */
    public push(value: T): void {
        this._values.add(value);
    }

    /**
     * Remove and return the value at the top of the stack.
     */
    public pop(): T {
        return this._values.removeLast();
    }

    /**
     * Return (but don't remove) the value at the top of the stack.
     */
    public peek(): T {
        return this._values.last();
    }
}

/**
 * A First-In-First-Out (FIFO) data structure.
 */
export class Queue<T> {
    private _values = new ArrayList<T>();

    /**
     * Get whether or not this queue has any values.
     */
    public any(): boolean {
        return this._values.any();
    }

    /**
     * Get the number of values that are in the queue.
     */
    public getCount(): number {
        return this._values.getCount();
    }

    /**
     * Get whether or not this queue contains the provided value using the optional comparison. If
     * The optional comparison is not provided, then === will be used.
     * @param value The value to search for.
     * @param comparison The optional comparison to compare values.
     */
    public contains(value: T, comparison?: (lhs: T, rhs: T) => boolean): boolean {
        return this._values.contains(value, comparison);
    }

    /**
     * Add the provided value to the start of this queue.
     * @param value The value to add to this queue.
     */
    public enqueue(value: T): void {
        this._values.add(value);
    }

    /**
     * Take the next value off of the end of this queue.
     */
    public dequeue(): T {
        return this._values.removeFirst();
    }
}

export function quote(value: string): string {
    let result: string;
    if (value === undefined) {
        result = "undefined";
    }
    else if (value === null) {
        result = "null";
    }
    else {
        result = `"${value}"`;
    }
    return result;
}

export function escape(documentText: string): string {
    let result: string = documentText;
    if (result) {
        let newResult: string = result;
        do {
            result = newResult;
            newResult = result.replace("\n", "\\n")
                .replace("\t", "\\t")
                .replace("\r", "\\r");
        }
        while (result !== newResult);
    }
    return result;
}

export function escapeAndQuote(text: string): string {
    return quote(escape(text));
}

export function toLowerCase(text: string): string {
    return text ? text.toLowerCase() : text;
}

export function isDefined(value: any): boolean {
    return value !== undefined && value !== null;
}

export function getLength(value: any[] | string): number {
    return isDefined(value) ? value.length : 0;
}

export function startsWith(value: string, prefix: string): boolean {
    return value && prefix && (prefix === value.substr(0, prefix.length));
}

export function startsWithIgnoreCase(value: string, prefix: string): boolean {
    return value && prefix && (prefix.toLowerCase() === value.substr(0, prefix.length).toLowerCase());
}

/**
 * Get whether or not the provided value ends with the provided suffix.
 * @param value The value to check.
 * @param suffix The suffix to look for.
 */
export function endsWith(value: string, suffix: string): boolean {
    return value && suffix && value.length >= suffix.length && value.substring(value.length - suffix.length) === suffix ? true : false;
}

/**
 * Get whether or not the provided value contains the provided searchString.
 * @param value The value to look in.
 * @param searchString The string to search for.
 */
export function contains(value: string, searchString: string): boolean {
    return value && searchString && value.indexOf(searchString) !== -1 ? true : false;
}

export function repeat(value: string, count: number): string {
    let result: string = "";
    if (value && count && count > 0) {
        for (let i = 0; i < count; ++i) {
            result += value;
        }
    }
    return result;
}

export function getLineIndex(value: string, characterIndex: number): number {
    let result: number;

    if (isDefined(value) && isDefined(characterIndex) && 0 <= characterIndex) {
        result = 0;
        for (let i = 0; i < characterIndex; ++i) {
            if (value[i] === "\n") {
                ++result;
            }
        }
    }

    return result;
}

export function getColumnIndex(value: string, characterIndex: number): number {
    let result: number;

    if (isDefined(value) && isDefined(characterIndex) && 0 <= characterIndex) {
        result = 0;
        for (let i = 0; i < characterIndex; ++i) {
            if (value[i] === "\n") {
                result = 0;
            }
            else {
                ++result;
            }
        }
    }

    return result;
}

export function getLineIndent(value: string, characterIndex: number): string {
    let result: string;

    const columnIndex: number = getColumnIndex(value, characterIndex);
    if (isDefined(columnIndex)) {
        let indentCharacterIndex: number = characterIndex - columnIndex;
        result = "";
        while (value[indentCharacterIndex] === " " || value[indentCharacterIndex] === "\t") {
            result += value[indentCharacterIndex];
            ++indentCharacterIndex;
        }
    }

    return result;
}

/**
 * A value that has a startIndex property.
 */
export interface HasStartIndex {
    startIndex: number;
}

/**
 * Get the start index of the provided values.
 * @param values
 */
export function getStartIndex(values: HasStartIndex[] | Iterable<HasStartIndex> | Iterator<HasStartIndex>): number {
    let result: number;
    if (values) {
        if (values instanceof Array) {
            if (values.length > 0) {
                result = values[0].startIndex;
            }
        }
        else {
            if (values.any()) {
                result = values.first().startIndex;
            }
        }
    }
    return result;
}

/**
 * A value that has an afterEndIndex property.
 */
export interface HasAfterEndIndex {
    afterEndIndex: number;
}

/**
 * Get the after end index of the provided values.
 * @param values
 */
export function getAfterEndIndex(values: HasAfterEndIndex[] | Iterable<HasAfterEndIndex>): number {
    let result: number;
    if (values) {
        if (values instanceof Array) {
            if (values.length > 0) {
                result = values[values.length - 1].afterEndIndex;
            }
        }
        else {
            if (values.any()) {
                result = values.last().afterEndIndex;
            }
        }
    }
    return result;
}

export function getSpan(values: HasStartIndexAndAfterEndIndex[] | Iterable<HasStartIndexAndAfterEndIndex>): Span {
    let result: Span;

    if (values) {
        if (values instanceof Array) {
            if (values.length > 0) {
                const startIndex: number = values[0].startIndex;
                const afterEndIndex: number = values[values.length - 1].afterEndIndex;
                result = new Span(startIndex, afterEndIndex - startIndex);
            }
        }
        else {
            if (values.any()) {
                const startIndex: number = values.first().startIndex;
                const afterEndIndex: number = values.last().afterEndIndex;
                result = new Span(startIndex, afterEndIndex - startIndex);
            }
        }
    }

    return result;
}

/**
 * An object that has a getLength() method.
 */
export interface HasGetLength {
    getLength(): number;
}

/**
 * Get the combined length of the values in the provided array.
 */
export function getCombinedLength(values: HasGetLength[] | Iterable<HasGetLength> | Iterator<HasGetLength>): number {
    let result: number = 0;
    if (values) {
        for (const value of values) {
            result += value.getLength();
        }
    }
    return result;
}

/**
 * A value that has startIndex and afterEndIndex properties.
 */
export interface HasStartIndexAndAfterEndIndex {
    startIndex: number;
    afterEndIndex: number;
}

/**
 * Get the combined length of the values in the provided array. This function assumes that the
 * values in the array don't have any gaps between them (the spans of the values are assumed to be
 * contiguous).
 */
export function getContiguousLength(values: HasStartIndexAndAfterEndIndex[] | Iterable<HasStartIndexAndAfterEndIndex>): number {
    let result: number;
    if (!values) {
        result = 0;
    }
    else {
        if (values instanceof Array) {
            result = values.length >= 1 ? values[values.length - 1].afterEndIndex - values[0].startIndex : 0;
        }
        else {
            result = values.any() ? values.last().afterEndIndex - values.first().startIndex : 0;
        }
    }
    return result;
}

/**
 * Get the combined text of the values in the provided array.
 */
export function getCombinedText(values: any[] | Iterable<any> | Iterator<any>): string {
    let result: string = "";
    if (values) {
        for (const value of values) {
            result += value.toString();
        }
    }
    return result;
}

/**
 * Create a deep copy of the provided value.
 */
export function clone<T>(value: T): T {
    let result: any;

    if (value === null ||
        value === undefined ||
        typeof value === "boolean" ||
        typeof value === "number" ||
        typeof value === "string") {
        result = value;
    }
    else if (value instanceof Array) {
        result = cloneArray(value);
    }
    else {
        result = {};
        for (const propertyName in value) {
            result[propertyName] = clone(value[propertyName]);
        }
    }

    return result;
}

export function cloneArray<T>(values: T[]): T[] {
    let result: T[];
    if (values === undefined) {
        result = undefined;
    }
    else if (values === null) {
        result = null;
    }
    else {
        result = [];
        for (const index in values) {
            result[index] = clone(values[index]);
        }
    }
    return result;
}

/**
 * Find the nearest package.json file by looking in the current directory and each of its
 * parent directories.
 */
export function getPackageJson(): any {
    const fileName: string = "package.json";
    let parentPath: string = __dirname;

    let packageJson: any = null;

    let packageJsonFilePath: string = "";
    while (parentPath && !packageJson) {
        const packageJsonFilePath: string = path.join(parentPath, fileName);
        if (fs.existsSync(packageJsonFilePath)) {
            packageJson = JSON.parse(fs.readFileSync(packageJsonFilePath, "utf-8"));
        }
        else {
            parentPath = path.dirname(parentPath);
        }
    }

    return packageJson
}

/**
 * A one-dimensional span object.
 */
export class Span {
    constructor(private _startIndex: number, private _length: number) {
    }

    /**
     * The inclusive index at which this Span starts.
     */
    public get startIndex(): number {
        return this._startIndex;
    }

    /**
     * The length/number of indexes that this Span encompasses.
     */
    public get length(): number {
        return this._length;
    }

    /**
     * The last index that is contained by this span.
     */
    public get endIndex(): number {
        return this.afterEndIndex - 1;
    }

    /**
     * The first index after this span that is not contained by this span.
     */
    public get afterEndIndex(): number {
        return this.startIndex + this.length;
    }

    /**
     * Convert this Span to its string representation.
     */
    public toString(): string {
        return `[${this.startIndex},${this.afterEndIndex})`;
    }
}

/**
 * The different types of lexes.
 */
export const enum LexType {
    LeftCurlyBracket,
    RightCurlyBracket,
    LeftSquareBracket,
    RightSquareBracket,
    LeftAngleBracket,
    RightAngleBracket,
    LeftParenthesis,
    RightParenthesis,
    Letters,
    SingleQuote,
    DoubleQuote,
    Digits,
    Comma,
    Colon,
    Semicolon,
    ExclamationPoint,
    Backslash,
    ForwardSlash,
    QuestionMark,
    Dash,
    Plus,
    EqualsSign,
    Period,
    Underscore,
    Ampersand,
    VerticalBar,
    Space,
    Tab,
    CarriageReturn,
    NewLine,
    CarriageReturnNewLine,
    Asterisk,
    Percent,
    Hash,
    Unrecognized
}

/**
 * An individual lex from a lexer.
 */
export class Lex {
    constructor(private _text: string, private _startIndex: number, private _type: LexType) {
    }

    /**
     * The character index that this lex begins on.
     */
    public get startIndex(): number {
        return this._startIndex;
    }

    public get afterEndIndex(): number {
        return this._startIndex + this.getLength();
    }

    public get span(): Span {
        return new Span(this._startIndex, this.getLength());
    }

    /**
     * The string value for this token.
     */
    public toString(): string {
        return this._text;
    }

    /**
     * The length of the text of this token.
     */
    public getLength(): number {
        return this._text.length;
    }

    /**
     * The type of this token.
     */
    public getType(): LexType {
        return this._type;
    }

    public isWhitespace(): boolean {
        switch (this._type) {
            case LexType.Space:
            case LexType.Tab:
            case LexType.CarriageReturn:
                return true;

            default:
                return false;
        }
    }

    public isNewLine(): boolean {
        switch (this._type) {
            case LexType.CarriageReturnNewLine:
            case LexType.NewLine:
                return true;

            default:
                return false;
        }
    }
}

export function LeftCurlyBracket(startIndex: number): Lex {
    return new Lex("{", startIndex, LexType.LeftCurlyBracket);
}

export function RightCurlyBracket(startIndex: number): Lex {
    return new Lex("}", startIndex, LexType.RightCurlyBracket);
}

export function LeftSquareBracket(startIndex: number): Lex {
    return new Lex("[", startIndex, LexType.LeftSquareBracket);
}

export function RightSquareBracket(startIndex: number): Lex {
    return new Lex("]", startIndex, LexType.RightSquareBracket);
}

export function LeftAngleBracket(startIndex: number): Lex {
    return new Lex("<", startIndex, LexType.LeftAngleBracket);
}

export function RightAngleBracket(startIndex: number): Lex {
    return new Lex(">", startIndex, LexType.RightAngleBracket);
}

export function LeftParenthesis(startIndex: number): Lex {
    return new Lex("(", startIndex, LexType.LeftParenthesis);
}

export function RightParenthesis(startIndex: number): Lex {
    return new Lex(")", startIndex, LexType.RightParenthesis);
}

export function SingleQuote(startIndex: number): Lex {
    return new Lex("'", startIndex, LexType.SingleQuote);
}

export function DoubleQuote(startIndex: number): Lex {
    return new Lex("\"", startIndex, LexType.DoubleQuote);
}

export function Comma(startIndex: number): Lex {
    return new Lex(",", startIndex, LexType.Comma);
}

export function Colon(startIndex: number): Lex {
    return new Lex(":", startIndex, LexType.Colon);
}

export function Semicolon(startIndex: number): Lex {
    return new Lex(";", startIndex, LexType.Semicolon);
}

export function ExclamationPoint(startIndex: number): Lex {
    return new Lex("!", startIndex, LexType.ExclamationPoint);
}

export function Backslash(startIndex: number): Lex {
    return new Lex("\\", startIndex, LexType.Backslash);
}

export function ForwardSlash(startIndex: number): Lex {
    return new Lex("/", startIndex, LexType.ForwardSlash);
}

export function QuestionMark(startIndex: number): Lex {
    return new Lex("?", startIndex, LexType.QuestionMark);
}

export function Dash(startIndex: number): Lex {
    return new Lex("-", startIndex, LexType.Dash);
}

export function Plus(startIndex: number): Lex {
    return new Lex("+", startIndex, LexType.Plus);
}

export function EqualsSign(startIndex: number): Lex {
    return new Lex("=", startIndex, LexType.EqualsSign);
}

export function Period(startIndex: number): Lex {
    return new Lex(".", startIndex, LexType.Period);
}

export function Underscore(startIndex: number): Lex {
    return new Lex("_", startIndex, LexType.Underscore);
}

export function Ampersand(startIndex: number): Lex {
    return new Lex("&", startIndex, LexType.Ampersand);
}

export function VerticalBar(startIndex: number): Lex {
    return new Lex("|", startIndex, LexType.VerticalBar);
}

export function Space(startIndex: number): Lex {
    return new Lex(" ", startIndex, LexType.Space);
}

export function Tab(startIndex: number): Lex {
    return new Lex("\t", startIndex, LexType.Tab);
}

export function CarriageReturn(startIndex: number): Lex {
    return new Lex("\r", startIndex, LexType.CarriageReturn);
}

export function NewLine(startIndex: number): Lex {
    return new Lex("\n", startIndex, LexType.NewLine);
}

export function CarriageReturnNewLine(startIndex: number): Lex {
    return new Lex("\r\n", startIndex, LexType.NewLine);
}

export function Asterisk(startIndex: number): Lex {
    return new Lex("*", startIndex, LexType.Asterisk);
}

export function Percent(startIndex: number): Lex {
    return new Lex("%", startIndex, LexType.Percent);
}

export function Hash(startIndex: number): Lex {
    return new Lex("#", startIndex, LexType.Hash);
}

export function Letters(text: string, startIndex: number): Lex {
    return new Lex(text, startIndex, LexType.Letters);
}

export function Digits(text: string, startIndex: number): Lex {
    return new Lex(text, startIndex, LexType.Digits);
}

/**
 * Create an unrecognized token with the provided character string.
 */
export function Unrecognized(character: string, startIndex: number): Lex {
    return new Lex(character, startIndex, LexType.Unrecognized);
}

/**
 * A lexer that will break up a character stream into a stream of lexes.
 */
export class Lexer extends IteratorBase<Lex> {
    private _iterator: StringIterator;
    private _characterStartIndexOffset: number;

    private _currentLex: Lex;

    constructor(text: string, startIndex: number = 0) {
        super();

        this._iterator = new StringIterator(text);
        this._characterStartIndexOffset = startIndex;
    }

    /**
     * Whether this object has started tokenizing its input stream or not.
     */
    public hasStarted(): boolean {
        return this._iterator.hasStarted();
    }

    /**
     * Get whether this tokenizer has a current token or not.
     */
    public hasCurrent(): boolean {
        return isDefined(this._currentLex);
    }

    /**
     * The current lex that has been lexed from the source character stream.
     */
    public getCurrent(): Lex {
        return this._currentLex;
    }

    private getCurrentCharacterStartIndex(): number {
        return this._iterator.currentIndex + this._characterStartIndexOffset;
    }

    private hasCurrentCharacter(): boolean {
        return this._iterator.hasCurrent();
    }

    private getCurrentCharacter(): string {
        return this._iterator.getCurrent();
    }

    private nextCharacter(): boolean {
        return this._iterator.next();
    }

    /**
     * Get the next lex in the stream.
     */
    public next(): boolean {
        if (!this.hasStarted()) {
            this.nextCharacter();
        }

        if (this.hasCurrentCharacter()) {
            const currentLexStartIndex: number = this.getCurrentCharacterStartIndex();
            switch (this.getCurrentCharacter()) {
                case "{":
                    this._currentLex = LeftCurlyBracket(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "}":
                    this._currentLex = RightCurlyBracket(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "[":
                    this._currentLex = LeftSquareBracket(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "]":
                    this._currentLex = RightSquareBracket(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "(":
                    this._currentLex = LeftParenthesis(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case ")":
                    this._currentLex = RightParenthesis(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "<":
                    this._currentLex = LeftAngleBracket(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case ">":
                    this._currentLex = RightAngleBracket(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case `"`:
                    this._currentLex = DoubleQuote(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case `'`:
                    this._currentLex = SingleQuote(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "-":
                    this._currentLex = Dash(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "+":
                    this._currentLex = Plus(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case ",":
                    this._currentLex = Comma(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case ":":
                    this._currentLex = Colon(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case ";":
                    this._currentLex = Semicolon(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "!":
                    this._currentLex = ExclamationPoint(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "\\":
                    this._currentLex = Backslash(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "/":
                    this._currentLex = ForwardSlash(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "?":
                    this._currentLex = QuestionMark(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "=":
                    this._currentLex = EqualsSign(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case ".":
                    this._currentLex = Period(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "_":
                    this._currentLex = Underscore(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "&":
                    this._currentLex = Ampersand(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case " ":
                    this._currentLex = Space(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "\t":
                    this._currentLex = Tab(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "\r":
                    if (!this.nextCharacter() || this.getCurrentCharacter() !== "\n") {
                        this._currentLex = CarriageReturn(currentLexStartIndex);
                    }
                    else {
                        this._currentLex = CarriageReturnNewLine(currentLexStartIndex);
                        this.nextCharacter();
                    }
                    break;

                case "\n":
                    this._currentLex = NewLine(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "*":
                    this._currentLex = Asterisk(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "%":
                    this._currentLex = Percent(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "|":
                    this._currentLex = VerticalBar(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                case "#":
                    this._currentLex = Hash(currentLexStartIndex);
                    this.nextCharacter();
                    break;

                default:
                    if (isLetter(this.getCurrentCharacter())) {
                        this._currentLex = Letters(readLetters(this._iterator), currentLexStartIndex);
                    }
                    else if (isDigit(this.getCurrentCharacter())) {
                        this._currentLex = Digits(readDigits(this._iterator), currentLexStartIndex);
                    }
                    else {
                        this._currentLex = Unrecognized(this.getCurrentCharacter(), currentLexStartIndex);
                        this.nextCharacter();
                    }
                    break;
            }
        }
        else {
            this._currentLex = undefined;
        }

        return this.hasCurrent();
    }
}

export function readWhile(iterator: Iterator<string>, condition: (character: string) => boolean): string {
    let result: string = iterator.getCurrent();

    while (iterator.next() && condition(iterator.getCurrent())) {
        result += iterator.getCurrent();
    }

    return result;
}

export function readLetters(iterator: Iterator<string>): string {
    return readWhile(iterator, isLetter);
}

export function readWhitespace(iterator: Iterator<string>): string {
    return readWhile(iterator, isWhitespace);
}

export function readDigits(iterator: Iterator<string>): string {
    return readWhile(iterator, isDigit);
}

export function isWhitespace(value: string): boolean {
    switch (value) {
        case " ":
        case "\t":
        case "\r":
            return true;

        default:
            return false;
    }
}

/**
 * Is the provided character a letter?
 */
export function isLetter(character: string): boolean {
    return ("a" <= character && character <= "z") ||
        ("A" <= character && character <= "Z");
}

/**
 * Is the provided character a digit?
 */
export function isDigit(character: string): boolean {
    return "0" <= character && character <= "9";
}

export function absoluteValue(value: number): number {
    return value < 0 ? value * -1 : value;
}

export class StringIterator extends IteratorBase<string> {
    private _currentIndex: number;
    private _started: boolean = false;
    private _step: number;

    constructor(private _text: string, private _startIndex: number = 0, private _endIndex: number = getLength(_text)) {
        super();

        this._currentIndex = _startIndex;
        this._step = _endIndex >= _startIndex ? 1 : -1;
    }

    public get currentIndex(): number {
        return this.hasCurrent() ? this._currentIndex : undefined;
    }

    public hasStarted(): boolean {
        return this._started;
    }

    public hasCurrent(): boolean {
        return this.hasStarted() && this.hasMore();
    }

    public getCurrent(): string {
        return this.hasCurrent() ? this._text[this._currentIndex] : undefined;
    }

    public next(): boolean {
        if (this._started === false) {
            this._started = true;
        }
        else if (this.hasMore()) {
            this._currentIndex += this._step;
        }

        return this.hasMore();
    }

    private hasMore(): boolean {
        return (this._step > 0) ? this._currentIndex < this._endIndex : this._currentIndex > this._endIndex;
    }
}

export class StringIterable extends IterableBase<string> {
    constructor(private _text: string) {
        super();
    }

    public iterate(): Iterator<string> {
        return new StringIterator(this._text, 0, getLength(this._text));
    }

    public iterateReverse(): Iterator<string> {
        return new StringIterator(this._text, getLength(this._text) - 1, -1);
    }
}
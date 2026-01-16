"use strict";

export interface Seq<T> extends Iterable<T> {
	map<U>(func: (t: T) => U): Seq<U>;
	flatMap<U>(func: (t: T) => Seq<U>): Seq<U>;
	filter(predicate: (t: T) => boolean): Seq<T>;
	distinct(): Seq<T>;
	distinctBy<U>(keySelector: (t: T) => U): Seq<T>;
	reduce<U>(initialValue: U, reducer: (left: U, right: T) => U): U;
	first(): T;
	last(): T;
	count(): number;
	take(n: number): Seq<T>;
	skip(n: number): Seq<T>;
	concat(other: Seq<T>): Seq<T>;
	zip<U, V>(other: Seq<U>, zipper: (t: T, u: U) => V): Seq<V>;
	toArray(): T[];
}

class SeqImpl<T> implements Seq<T> {
	[Symbol.iterator]: () => Iterator<T>;
	
	constructor(iteratorFunc: () => Iterator<T>) {
		this[Symbol.iterator] = iteratorFunc;
	}
	
	map<U>(func: (t: T) => U): Seq<U> {
		const self = this;
		return new SeqImpl<U>(function * () {
			for (let t of self) {
				yield func(t);
			}
		});
	}
	
	flatMap<U>(func: (t: T) => Seq<U>): Seq<U> {
		const self = this;
		return new SeqImpl<U>(function * () {
			for (let seq of self) {
				for (let u of func(seq)) {
					yield u;
				}
			}
		});
	}
	
	filter(predicate: (t: T) => boolean): Seq<T> {
		const self = this;
		return new SeqImpl<T>(function * () {
			for (let t of self) {
				if (predicate(t)) {
					yield t;
				}
			}
		});
	}
	
	distinct(): Seq<T> {
		const set = new Set<T>();
		const self = this;
		return new SeqImpl<T>(function * (){
			for (let t of self) {
				if (!set.has(t)) {
					set.add(t);
					yield t;
				}
			}
		});
	}
	
	distinctBy<U>(keySelector: (t: T) => U): Seq<T> {
		const set = new Set<U>();
		const self = this;
		return new SeqImpl<T>(function*(){
			for (let t of self) {
				const key = keySelector(t);
				if (!set.has(key)) {
					set.add(key);
					yield t;
				}
			}
		});
	}
	
	reduce<U>(initialValue: U, reducer: (left: U, right: T) => U): U {
		let left = initialValue;
		for (let right of this) {
			left = reducer(left, right);
		}
		
		return left;
	}
	
	first(): T {
		for (let t of this) {
			return t;
		}
		
		return undefined;
	}
	
	last(): T {
		let temp: T = undefined;
		for (let t of this) {
			temp = t;
		}
		
		return temp;
	}
	
	count(): number {
		let n = 0;
		for (let t of this) {
			++n;
		}
		
		return n;
	}
	
	take(n: number): Seq<T> {
		const self = this;
		return new SeqImpl<T>(function * (){
			let taken = 0;
			for (let t of self){
				if (taken < n) {
					++taken;
					yield t;
				}
			}	
		});
	}
	
	skip(n: number): Seq<T> {
		const self = this;
		return new SeqImpl<T>(function * (){
			let skipped = 0;
			for (let t of self){
				if (skipped < n) {
					++skipped;
				}
				else {
					yield t;
				}
			}	
		});
	}
	
	concat(other: Seq<T>): Seq<T> {
		const self = this;
		return new SeqImpl<T>(function * (){
			for (let t of self) {
				yield t;
			}
			
			for (let t of other) {
				yield t;
			}
		});
	}
	
	zip<U, V>(other: Seq<U>, zipper: (t: T, u: U) => V): Seq<V>{
		const self = this;
		return new SeqImpl<V>(function*(){
			let seqGen = other[Symbol.iterator]();
			for (let x of self) {
				let other = seqGen.next();
				if (other.done)
					break;
				else
					yield zipper(x, other.value);
			}
		});
	}
	
	toArray(): T[] {
		const array: T[] = [];
		for (let t of this) {
			array.push(t);
		}
		
		return array;
	}
}

export default class Sequence {
	static fromArray<T>(array: T[]): Seq<T> {
		return new SeqImpl<T>(function * (){
			for (let i = 0; i < array.length; ++i) {
				yield array[i];
			}
		})
	}
	
	static fromGenerator<T>(gen: () => Iterator<T>): Seq<T> {
		return new SeqImpl<T>(gen);
	}
	
	static empty<T>() : Seq<T> {
		return new SeqImpl<T>(function * () {});
	}
	
	static just<T>(value: T): Seq<T>{
		return new SeqImpl<T>(function * (){
			yield value;
		});
	}
	
	static repeat<T>(value: T, n: number): Seq<T> {
		return new SeqImpl<T>(function * (){
			for (let i = 0; i < n; ++i) {
				yield value;
			}
		})
	}
	
	static range(nFrom: number, nTo: number) {
		return new SeqImpl<number>(function * (){
			for (let i = nFrom; i < nTo; ++i) {
				yield i;
			}
		});
	}
	
	static inifinit(): Seq<number> {
		return new SeqImpl<number>(function * (){
			for (let i = 0; true; ++i) {
				yield i;
			}
		});
	}
}
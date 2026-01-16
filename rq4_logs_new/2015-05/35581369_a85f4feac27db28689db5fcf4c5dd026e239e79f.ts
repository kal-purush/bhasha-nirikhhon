/**
 * Maybe types and functions on them.
 * 
 * @module maybe
 */

'use strict';

import Tuple from 'tuple';
export default Maybe;

/** 
 * Values which explicitly may not actually be there.
 * 
 * For any type A, values of Maybe<A> are either values of A or "Nothing" at all.
 * In JavaScript values of type A *always* may contain null, so Maybe<A> may seem
 * to be redundant---and it is!
 * 
 * The genuine value of Maybe arises when we assume that for all types A, all values
 * encountered throughout some "safe" zone within your program are *never* null. 
 * This invariant thus ensures that values (a:A) are always "real" values and now 
 * Maybe is the only way to encode "possibly missingness".
 * 
 * From here, types can help us to ensure that we never forget to handle a null.
 */
class Maybe<A> {
  
  /** 
   * Eliminate a Maybe<A> value by providing continuations for both the case where
   * a value is there (just) and when it is not (nothing).
   */
  fold<R>(match: { just: (a: A) => R, nothing: R }): R;
  
  /** Test to determine whether or not a value is actually contained in the Maybe. */
  isJust(): boolean;
  
  /** Pass a function into the Maybe container and change the value inside. */
  map<B>(fn: (a: A) => B): Maybe<B> { return Maybe.map(fn)(this); };
  
  /** Given two Maybes, produce a Tuple if their values if both values exist. */
  zip<B>(mb: Maybe<B>): Maybe<Tuple<A, B>> { return Maybe.zip(this, mb); };
  
  /** Given two Maybes, combine their values using a given function if they both exist. */
  zipWith<B, R>(mb: Maybe<B>, fn: (a: A, b: B) => R): Maybe<R> { return Maybe.zipWith(fn)(this, mb); };
  
  /** Lift a computation which might fail on to values which might not exist. */
  bind<B>(fn: (a: A) => Maybe<B>): Maybe<B> { return Maybe.bind(fn)(this); };
  
  /** If this value is Nothing, try the next one. */
  or(m: Maybe<A>): Maybe<A> { return Maybe.or(this, m); };
  
  /** 
   * If A has a default value then we can use it to convert a value of (k: Maybe<A>)
   * into either the default value (if k was Nothing) or the contained value (if k 
   * was Just).
   */
  orElse(def: A): A {
    return this.fold({ just: id, nothing: def });
  }
}

function id<A>(a: A): A { return a; }

module Maybe {
  
  export interface Elim<A, R> {
    just: (a: A) => R;
    nothing: R;
  }
 
  export class Just<A> extends Maybe<A>  { 
    constructor(public value: A) { super(); };
    isJust() { return true; }
    fold<R>(match: Elim<A, R>) { return match.just(this.value); }
  }
  
  export class Nothing<A> extends Maybe<A> { 
    constructor() { super(); };
    isJust() { return false; }
    fold<R>(match: Elim<A, R>) { return match.nothing; } 
  }
    
  /** Inject a value of A into a Maybe. */
  export function of<A>(value: A): Maybe<A> {
    return new Just(value);
  }
  
  /** 
   * Create an empty value of Maybe. Typically this must be annotated with
   * the type A. 
   */
  export function zero<A>(): Nothing<A> {
    return new Nothing<A>();
  }
  
  /** 
   * Eliminate a Maybe<A> value by providing continuations for both the case where
   * a value is there (just) and when it is not (nothing).
   */
  export function fold<A, R>(match: { just: (a: A) => R, nothing: R }): (m: Maybe<A>) => R {
    return function eliminator(m: Maybe<A>) { return m.fold(match); }
  } 
  
  /** Pass a function into the Maybe container and change the value inside. */
  export function map<A, B>(fn: (a: A) => B): (m: Maybe<A>) => Maybe<B> {
    return function morphism(ma) {
      return ma.fold({
        just: (a) => { return new Just(fn(a)) },
        nothing: zero<B>()
      });
    }
  }
  
  /** Lift a computation which might fail on to values which might not exist. */
  export function bind<A, B>(fn: (a: A) => Maybe<B>): (m: Maybe<A>) => Maybe<B> {
    return function morphism(ma) {
      return ma.fold({
        just: (a) => { return fn(a) },
        nothing: zero<B>()
      })
    }
  }




  
  /** Given two Maybes, combine their values using a given function if they both exist. */
  export function zipWith<A, B, R>(fn: (a: A, b: B) => R): ((ma: Maybe<A>, mb: Maybe<B>) => Maybe<R>) {
    return function(ma, mb) {
      return ma.fold({
        just: (a) => { return mb.fold({
          just: (b) => { return of(fn(a, b)); },
          nothing: zero<R>()
        })},
        nothing: zero<R>()
      })
    } 
  }


  
  /** Given two Maybes, produce a Tuple if their values if both values exist. */  
  export function zip<A, B>(ma: Maybe<A>, mb: Maybe<B>): Maybe<Tuple<A, B>> {
    return zipWith<A, B, Tuple<A, B>>(Tuple.of)(ma, mb);
  }

  /** Apply a function inside of a Maybe allowing for failure. */    
  export function ap<A, B>(mf: Maybe<(a: A) => B>, ma: Maybe<A>): Maybe<B> {
    return mf.fold({
      just: (f) => { return ma.fold({
        just: (a) => { return of(f(a)); },
        nothing: zero<B>()
      })},
      nothing: zero<B>()
    })
  }

  /** If this value is Nothing, try the next one. */
  export function or<A>(m1: Maybe<A>, m2: Maybe<A>): Maybe<A> {
    return m1.fold({
      just: (_) => { return m1; },
      nothing: m2
    })
  }
  
  /** Collapse a set of maybes into the leftmost succeeding one. */
  export function leftmost<A>(...maybes: Array<Maybe<A>>): Maybe<A> {
    maybes.forEach((v) => {
      if (v.isJust()) { return v; }
    })
    return zero<A>();
  }
  
  /** Collapse a set of maybes into the rightmost succeeding one. */
  export function rightmost<A>(...maybes: Array<Maybe<A>>): Maybe<A> {
    // Iterate from the end of the array forward
    for (var i = maybes.length; 0 >= i; i--) {
      var x = maybes[i];  
      if (x.isJust()) { return x; } 
    }
    return zero<A>();
  }
  
  /** A boolean interpreted as a value of Maybe<{}>. */
  export function guard(b: boolean): Maybe<{}> { 
    if (b) { return Maybe.of({}) } 
    else { return Maybe.zero() } 
  };

}
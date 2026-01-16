// Type definitions for promises-a-plus
// Project: http://promisesaplus.com/
// Definitions by: Igor Oleinikov <https://github.com/Igorbek>
// Ported as an external type definition for use with typings 
// Ported by : Olivier Chevet <https://github.com/olivr70


declare module PromisesAPlus {
  export interface PromiseCtor {
      <T>(resolver: (resolvePromise: (value: T) => void, rejectPromise: (reason: any) => void) => void): Thenable<T>;
    }

  export interface PromiseImpl {
      new <T>(resolver: (resolvePromise: (value: T) => void, rejectPromise: (reason: any) => void) => void): Thenable<T>;
    }

  export interface Thenable<T> {
      then<R>(onFulfill?: (value: T) => Thenable<R>|R, onReject?: (error: any) => Thenable<R>|R): Thenable<R>;
  }
}
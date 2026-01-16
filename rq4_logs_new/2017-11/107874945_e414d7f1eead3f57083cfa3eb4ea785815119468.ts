
declare module 'promise-polyfill' {
  interface Thenable<T> {
    then<U>(onFulfilled?: (value: T) => U | Thenable<U>, onRejected?: (error: any) => U | Thenable<U>): Thenable<U>;
    then<U>(onFulfilled?: (value: T) => U | Thenable<U>, onRejected?: (error: any) => void): Thenable<U>;
  }

  class Promise<T> implements Thenable<T> {
    _immediateFn: Function | undefined;

    constructor(callback: (resolve : (value?: T | Thenable<T>) => void, reject: (error?: any) => void) => void);

    then<U>(onFulfilled?: (value: T) => U | Thenable<U>, onRejected?: (error: any) => U | Thenable<U>): Promise<U>;
    then<U>(onFulfilled?: (value: T) => U | Thenable<U>, onRejected?: (error: any) => void): Promise<U>;

    catch<U>(onRejected?: (error: any) => U | Thenable<U>): Promise<U>;
  }

  var localPromise: Promise<any>;

  export = localPromise;
}
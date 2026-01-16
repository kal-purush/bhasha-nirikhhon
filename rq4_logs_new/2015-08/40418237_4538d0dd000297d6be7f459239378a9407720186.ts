import { OnComplete, OnError, OnNext } from '../types';
import { AbstractObservable, Generator, Observer } from '../core';
import { Scheduler } from './async_scheduler';

export abstract class Observable<T> implements AbstractObservable<T> {

  isDisposed: boolean;
  _scheduler: Scheduler;

  constructor() {
    this.isDisposed = false;
    this._scheduler = new Scheduler();
  }

  static create<E>(cb: Generator<E>): Observable<E> {
    return new ColdObservable<E>(cb);
  }

  static defer<E>(cb: Generator<E>): Observable<E> {
    return new DeferredObservable<E>(cb);
  }

  static publish<E>(cb: Generator<E>): Observable<E> {
    return new HotObservable<E>(cb);
  }
  
  abstract dispose(): void;
  
  abstract subscribe(subscriber: Observer<T>): void;

  subscribeOnComplete(complete: OnComplete): void {
    let observer: Observer<T> = {
      complete: complete,
      error: this._throw,
      next: () => {}
    };
    this.subscribe(observer);
  }

  subscribeOnError(error: OnError): void {
    let observer: Observer<T> = {
      complete: () => {},
      error: error,
      next: () => {}
    };
    this.subscribe(observer);
  }

  subscribeOnNext(next: OnNext<T>): void {
    let observer: Observer<T> = {
      complete: () => {},
      error: this._throw,
      next: next
    };
    this.subscribe(observer);
  }

  _assertNoComplete(): Error {
    return new Error('An observer called complete after it already completed.');
  }

  _assertNoError(): Error {
    return new Error('An observer called error after it completed.');
  }

  _assertNoNext(): Error {
    return new Error('An observer called next after it completed.');
  }

  _assertNoSubscribe(): Error {
    return new Error('Called subscribe on an observable that already completed.');
  }

  _throw(err: any): void { throw err; }

}

export class ColdObservable<T> extends Observable<T> {

  constructor(private _cb: Generator<T>) { super(); }
  
  dispose(): void { this.isDisposed = true; }
  
  subscribe(subscriber: Observer<T>): void {
    if (this.isDisposed) {
      subscriber.error(this._assertNoSubscribe());
      return;
    }
    let observer: Observer<T> = {
      complete: () => {
        if (this.isDisposed) {
          subscriber.error(this._assertNoComplete());
          return;
        }
        this.dispose();
        this._scheduler.schedule(() => {
          try { subscriber.complete(); }
          catch(err) { subscriber.error(err); }
        });
      },
      error: err => {
        if (this.isDisposed) {
          subscriber.error(this._assertNoError());
          return;
        }
        this._scheduler.schedule(() => subscriber.error(err));
      },
      next: object => {
        if (this.isDisposed) {
          subscriber.error(this._assertNoNext());
          return;
        }
        this._scheduler.schedule(() => {
          try { subscriber.next(object); }
          catch(err) { subscriber.error(err); }
        });
      }
    };
    setTimeout(() => {
      try { this._cb(observer); }
      catch(err) { observer.error(err); }
    });
  }
  
}

export class DeferredObservable<T> extends Observable<T> {

  _subscriptions: Observer<T>[];

  constructor(private _cb: Generator<T>) { 
    super();
    this._subscriptions = [];
  }
  
  dispose(): void {
    this._subscriptions.forEach(subscriber => setTimeout(() => {
      try { subscriber.complete(); }
      catch(err) { subscriber.error(err); }
    }));
    this._subscriptions = [];
  }
  
  subscribe(subscriber: Observer<T>): void {
    if (this.isDisposed) {
      subscriber.error(this._assertNoSubscribe());
      return;
    }
    this._subscriptions.push(subscriber);
    if (this._subscriptions.length === 1) this._publish();
  }
  
  _publish(): void {
    let observer: Observer<T> = {
      complete: () => {
        if (this.isDisposed) throw this._assertNoComplete();
        this.isDisposed = true;
        this._scheduler.schedule(() => this.dispose());
      },
      error: err => {
        if (this.isDisposed) throw this._assertNoError();
        this._scheduler.schedule(() => {
          this._subscriptions.forEach(subscriber => setTimeout(() => subscriber.error(err)));
        });
      },
      next: object => {
        if (this.isDisposed) throw this._assertNoNext();
        this._scheduler.schedule(() => {
          this._subscriptions.forEach(subscriber => setTimeout(() => {
            try { subscriber.next(object); }
            catch(err) { subscriber.error(err); }
          }));
        });
      }
    };
    setTimeout(() => {
      try { this._cb(observer); }
      catch(err) { observer.error(err); }
    });
  }

}

export class HotObservable<T> extends DeferredObservable<T> {

  constructor(cb: Generator<T>) {
    super(cb);
    this._publish();
  }
  
  subscribe(subscriber: Observer<T>): void { 
    if (this.isDisposed) {
      subscriber.error(this._assertNoSubscribe());
      return;
    }
    this._subscriptions.push(subscriber);
  }
  
}

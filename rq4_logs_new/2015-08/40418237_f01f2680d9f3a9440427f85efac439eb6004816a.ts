import { AbstractSubject, Observer } from '../core';
import { Observable } from './async_observable';

export class Subject<T> extends Observable<T> implements AbstractSubject<T> {

  _subscribers: Observer<T>[];
  _subscriptions: Observable<T>[];

  constructor() {
    super();
    this._subscribers = [];
    this._subscriptions = [];
  }

  complete(subscription?: Observable<T>): void {
    if (this.isDisposed) {
      this.error(this._assertNoComplete());
      return;
    }
    this.unsubscribeFrom(subscription);
  }

  dispose(): void {
    this._scheduler.schedule(() => {
      this.isDisposed = true;
      this._subscribers.forEach(subscriber => setTimeout(() => {
        try { subscriber.complete(); }
        catch(err) { subscriber.error(err); }
      }));
      this._subscribers = [];
    });
  }

  error(err: any): void {
    this._scheduler.schedule(() => {
      this._subscribers.forEach(subscriber => setTimeout(() => subscriber.error(err)));
    });
  }

  next(object?: T): void {
    if (this.isDisposed) {
      this.error(this._assertNoNext());
      return;
    }
    this._scheduler.schedule(() => {
      this._subscribers.forEach(subscriber => setTimeout(() => {
        try { subscriber.next(object); }
        catch(err) { subscriber.error(err); }
      }));
    });
  }

  subscribe(subscriber: Observer<T>): void { 
    if (this.isDisposed) {
      subscriber.error(this._assertNoSubscribe());
      return;
    }
    this._subscribers.push(subscriber);
    this._subscriptions.forEach(subscription => subscription.subscribe(this));
  }

  subscribeTo(subscription: Observable<T>): void {
    this._subscriptions.push(subscription);
    if (this.isDisposed) this.isDisposed = false;
  }

  unsubscribeFrom(subscription: Observable<T>): void {
    let idx: number = this._subscriptions.indexOf(subscription);
    if (idx > -1) this._subscriptions.splice(idx, 1);
    if (this._subscriptions.length === 0) this.dispose();
  }

}

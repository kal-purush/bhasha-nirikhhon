import { Callback } from '../types';
import { AbstractScheduler } from '../core';

export class Scheduler implements AbstractScheduler {

  _queue: Callback[];

  constructor() { this._queue = []; }

  schedule(cb: Callback): void {
    this._queue.push(cb);
    if (this._queue.length === 1) this._scheduleSubscription();
  }

  tick(): void {
    this._queue.splice(0, 1);
    if (this._queue.length > 0) this._scheduleSubscription();
  }

  _scheduleSubscription(): void {
    setTimeout(() => {
      this._queue[0]();
      setTimeout(() => this.tick());
    });
  }

}

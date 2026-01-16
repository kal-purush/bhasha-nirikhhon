import events = require('events')

declare class Batch <T> extends events.EventEmitter {

  on (name: 'progress', listener: (event: Batch.ProgressEvent<T>) => any): this;
  on (name: string, listener: Function): this;
  concurrency (count: number): this;
  throws (throws: boolean): this;
  push (fn: (done: Batch.Callback<T>) => any): this;
  end (cb: Batch.Callback<T[]>): this;

}

declare namespace Batch {
  export interface Callback <T> {
    (err: Error, result?: T): any
  }

  export interface ProgressEvent <T> {
    index: number;
    value: T;
    error: any;
    pending: number;
    total: number;
    complete: number;
    percent: number;
    start: Date;
    end: Date;
    duration: number;
  }
}

export = Batch;
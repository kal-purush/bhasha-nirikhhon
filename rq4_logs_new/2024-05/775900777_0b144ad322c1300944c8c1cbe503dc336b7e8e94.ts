/* eslint-disable max-classes-per-file */

export function Try<T>(func: () => T): Result<T, unknown> {
  try {
    return new Success(func());
  } catch (e) {
    return new Failure(e);
  }
}

interface BaseResult<T, E> {
  isSuccess(): this is Success<T, E>;
  isFailure(): this is Failure<T, E>;
  map<U>(fn: (val: T) => U): Result<U, E>;
  bind<U>(fn: (val: T) => Result<U, E>): Result<U, E>;

  mapFail<U>(fn: (err: E) => U): Result<T, U>;
  bindFail<U>(fn: (err: E) => Result<T, U>): Result<T, U>;

  unwrap(): T | never;
  unwrapError(): E | never;
  unwrapOr(optb: T): T;
  unwrapOrElse(op: (err: E) => T): T;
}

export class Failure<T, E> implements BaseResult<T, E> {
  type: 'fail';

  error: E;

  constructor(error: E) {
    this.type = 'fail';
    this.error = error;
  }

  // eslint-disable-next-line class-methods-use-this
  isSuccess(): this is Success<T, E> {
    return false;
  }

  // eslint-disable-next-line class-methods-use-this
  isFailure(): this is Failure<T, E> {
    return true;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  map<U>(fn: (val: T) => U): Result<U, E> {
    return this as unknown as Failure<U, E>;
  }

  mapFail<U>(fn: (err: E) => U): Result<T, U> {
    return new Failure(fn(this.error));
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  bind<U>(fn: (val: T) => Result<U, E>): Result<U, E> {
    return this as unknown as Failure<U, E>;
  }

  bindFail<U>(fn: (err: E) => Result<T, U>): Result<T, U> {
    return fn(this.error);
  }

  // eslint-disable-next-line class-methods-use-this
  unwrap(): T {
    throw new Error('Cannot unwrap failure');
  }

  // eslint-disable-next-line class-methods-use-this
  unwrapError(): E {
    return this.error;
  }

  // eslint-disable-next-line class-methods-use-this
  unwrapOr(optb: T): T {
    return optb;
  }

  unwrapOrElse(op: (err: E) => T): T {
    return op(this.error);
  }
}

export class Success<T, E> implements BaseResult<T, E> {
  type: 'success';

  value: T;

  constructor(value: T) {
    this.type = 'success';
    this.value = value;
  }

  // eslint-disable-next-line class-methods-use-this
  isSuccess(): this is Success<T, E> {
    return true;
  }

  // eslint-disable-next-line class-methods-use-this
  isFailure(): this is Failure<T, E> {
    return false;
  }

  map<U>(fn: (val: T) => U): Result<U, E> {
    return new Success<U, E>(fn(this.value));
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  mapFail<U>(fn: (err: E) => U): Result<T, U> {
    return this as unknown as Success<T, U>;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  bind<U>(fn: (val: T) => Result<U, E>): Result<U, E> {
    return fn(this.value);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  bindFail<U>(fn: (err: E) => Result<T, U>): Result<T, U> {
    return this as unknown as Success<T, U>;
  }

  unwrap(): T {
    return this.value;
  }

  // eslint-disable-next-line class-methods-use-this
  unwrapError(): E {
    throw new Error('TypeError : Success is NOT Failure');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  unwrapOr(_: T): T {
    return this.value;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  unwrapOrElse(_: (err: E) => T): T {
    return this.value;
  }
}

export type Result<T, E> = Success<T, E> | Failure<T, E>;
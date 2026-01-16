const DEFAULT_INTERVAL_MS = 1000;
const DEFAULT_TIMEOUT_MS = 10000;

export class StrongPromise<T> {
  private readonly executor: (resolve: (value?: T | PromiseLike<T>) => void, reject: (reason?: any) => void) => void;
  private readonly interval: number;
  private readonly timeout: number;
  private readonly startTime: number;
  private readonly promise: Promise<T>;

  constructor(
    executor: (resolve: (value?: T | PromiseLike<T>) => void, reject: (reason?: any) => void) => void,
    options: {
      interval?: number;
      timeout?: number;
    } = {},
  ) {
    const {
      interval = DEFAULT_INTERVAL_MS,
      timeout = DEFAULT_TIMEOUT_MS,
    } = options;

    this.executor = executor;
    this.interval = interval;
    this.timeout = timeout;
    this.startTime = Date.now();
    this.promise = this.createPromise();
  }

  public then(
    onfulfilled?: ((value: T) => T | PromiseLike<T>) | undefined | null,
    onrejected?: ((reason: any) => T | PromiseLike<T>) | undefined | null,
  ): Promise<T>;
  public then<TResult>(
    onfulfilled: ((value: T) => T | PromiseLike<T>) | undefined | null,
    onrejected: (reason: any) => TResult | PromiseLike<TResult>,
  ): Promise<T | TResult>;
  public then<TResult>(
    onfulfilled: (value: T) => TResult | PromiseLike<TResult>,
    onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null,
  ): Promise<TResult>;
  public then<TResult1, TResult2>(
    onfulfilled: (value: T) => TResult1 | PromiseLike<TResult1>,
    onrejected: (reason: any) => TResult2 | PromiseLike<TResult2>,
  ): Promise<TResult1 | TResult2> {
    return this.promise.then(onfulfilled, onrejected);
  }

  public catch(onrejected?: ((reason: any) => T | PromiseLike<T>) | undefined | null): Promise<T>;
  public catch<TResult>(onrejected: (reason: any) => TResult | PromiseLike<TResult>): Promise<T | TResult> {
    return this.promise.catch(onrejected);
  }

  private createPromise(): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      new Promise<T>(this.executor).then(resolve, (lastReason) => {
        const now = Date.now();
        if (now - this.startTime >= this.timeout) {
          reject(lastReason);
        } else {
          setTimeout(() => this.createPromise().then(resolve, reject), this.interval);
        }
      });
    });
  }
}
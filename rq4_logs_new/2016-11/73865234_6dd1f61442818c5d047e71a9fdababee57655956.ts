class Task<T> {
  private fun: Function;
  private asyncStyle: NaiveScheduler.AsyncStyle;
  public promise: Promise<T>;
  private resolve: (value: any) => void;  // `T | Thenable<T>` is hard to type
  private reject: (error: any) => void;

  constructor(fun: Function, asyncStyle: NaiveScheduler.AsyncStyle) {
    this.fun = fun;
    this.asyncStyle = asyncStyle != NaiveScheduler.AsyncStyle.INFER ?
        asyncStyle :
        fun.length == 0 ? NaiveScheduler.AsyncStyle.PROMISE : NaiveScheduler.AsyncStyle.TWO_CBS;
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }

  run(): Promise<void> {
    process.nextTick(() => {
      try {
        switch (this.asyncStyle) {
          case NaiveScheduler.AsyncStyle.PROMISE:
            return this.fun().then(this.resolve, this.reject);
          case NaiveScheduler.AsyncStyle.TWO_CBS:
            return this.fun(this.resolve, this.reject);
          case NaiveScheduler.AsyncStyle.DOT_FAIL:
            (this.resolve as any).fail = this.reject;
            return this.fun(this.resolve);
          case NaiveScheduler.AsyncStyle.NODE:
            return this.fun((error: any, value: any) => {
              if (error == null) {
                this.resolve(value);
              } else {
                this.reject(error);
              }
            });
        }
      } catch (e) {
        this.reject(e);
      }
    });
    return this.promise.then(() => {}, () => {});
  }
}


class NaiveScheduler {
  private tasks: Task<any>[];
  private currentTask: number;
  private promise: Promise<void>;
  private onComplete: () => void;

  constructor() {
    this.tasks = [];
  }

  /**
   * Schedule a function to be run.  Functions are run one at a time, meaning that one must
   * complete before the next one starts.  Functions are run in the order in which they are
   * scheduled.
   *
   * @param {Function} fun The function to schedule
   * @param {AsyncStyle=} asyncStyle Describes how the function singals when it is complete, if it
   *     failed, and what value it'd like to return. @see NaiveScheduler.AsyncStyle for details.
   *
   * @returns {Promise.<T>} A promise which will resolve/reject to the function's value
   */
  schedule<T>(fun: Function, asyncStyle = NaiveScheduler.AsyncStyle.INFER): Promise<T> {
    var task = new Task<T>(fun, asyncStyle);
    this.tasks.push(task);

    if (this.currentTask == undefined) {
      this.promise = new Promise<void>((resolve) => {
        this.onComplete = resolve;
      });
      this.runNextTask();
    }

    return task.promise;
  }

  /**
   * Gets a promise which resolves once all the scheduled tasks are complete.
   *
   * @returns {Promise.<void>}
   */
  complete(): Promise<void> {
    if (this.promise != null) {
      return this.promise;
    } else {
      return new Promise<void>((resolve) => {
        process.nextTick(resolve);
      });
    }
  }

  private runNextTask(): void {
    this.currentTask = (this.currentTask + 1) || 0;
    if (this.currentTask >= this.tasks.length) {
      this.tasks.length = 0;
      this.currentTask = undefined;
      this.onComplete();
      this.promise = undefined;
    } else {
      this.tasks[this.currentTask].run().then(() => {
        this.runNextTask();
      });
    }
  }
}

module NaiveScheduler {
  /**
   * Flags to signifiy how a function implements asynchronous behavior
   * @enum {number}
   * @prop {number} PROMISE Function returns a promise
   * @prop {number} TWO_CBS Function takes two callback functions, the first for success and the
   *     second for failure.
   * @prop {number} DOT_FAIL Function takes one callback function, which is used for success.  This
   *     callback function has a property `fail`, which is the callback function for failure
   * @prop {number} NODE Function takes one callback function.  If the first argument to that
   *     callback funtion is `null` or `undefined`, that signifies success and the second argument
   *     should be used for the return value.  If the first argument is something else, that
   *     signifies failure and the first argument is the error to return.
   * @prop {number} INFER Function may or may not take a callback function.  If it takes one or
   *     more arguments, assume it uses `TWO_CBS` style.  Otherwise, it uses `NODE` style.
   */
  export enum AsyncStyle {PROMISE, TWO_CBS, DOT_FAIL, NODE, INFER = undefined}
}

export = NaiveScheduler;
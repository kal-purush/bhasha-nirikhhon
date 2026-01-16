export interface Transaction {
  execute(query: string): Promise<any>;
}

export interface Executer<T> {
  execute(action: DbAction<T>): Promise<T>;
}

export class DbAction<T> {
  private readonly _action: (tx: Transaction) => Promise<T>;

  static fromQuery<T>(query: string): DbAction<T> {
    return new this((tx) => tx.execute(query));
  }

  static resolve<T>(x: T): DbAction<T> {
    return new this((tx) => Promise.resolve(x));
  }

  static reject<T>(x: Error): DbAction<T> {
    return new DbAction<T>(tx => Promise.reject(x));
  }

  constructor(action: (tx: Transaction) => Promise<T>) {
    this._action = action;

    Object.freeze(this);
  }

  execute(tx: Transaction): Promise<T> {
    return this._action(tx);
  }

  map<R>(f: (x: T) => R): DbAction<R> {
    return new DbAction<R>(tx => {
      return this.execute(tx).then(f);
    });
  }
}
export interface Transaction {
    execute(query: string): Promise<any>;
}
export interface Executer<T> {
    execute(action: DbAction<T>): Promise<T>;
}
export declare class DbAction<T> {
    private readonly _action;
    static fromQuery<T>(query: string): DbAction<T>;
    static resolve<T>(x: T): DbAction<T>;
    static reject<T>(x: Error): DbAction<T>;
    constructor(action: (tx: Transaction) => Promise<T>);
    execute(tx: Transaction): Promise<T>;
    map<R>(f: (x: T) => R): DbAction<R>;
}
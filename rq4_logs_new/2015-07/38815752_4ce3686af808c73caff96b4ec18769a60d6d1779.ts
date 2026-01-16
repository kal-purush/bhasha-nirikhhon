declare class Promise {
    constructor(func?: (resolve, reject) => any);
    private data;
    private onResolve;
    private onReject;
    resolved: boolean;
    rejected: boolean;
    done: boolean;
    private ret;
    private set(data?);
    resolve(data?: any): void;
    private _resolve();
    reject(data?: any): void;
    private _reject();
    then(res: Function, rej?: Function): Promise;
    catch(cb: Function): void;
    static all(arr: Array<Promise>): Promise;
    static chain(arr: Array<Promise>): Promise;
    static create(obj: any): Promise;
}
export = Promise;
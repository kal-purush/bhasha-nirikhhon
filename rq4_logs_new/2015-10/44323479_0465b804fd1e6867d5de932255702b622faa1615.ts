declare namespace AsyncChainer {
    var Cancellation: any;
    interface ContractOptionBag {
        /** Reverting listener for a contract. This will always be called after a contract gets finished in any status. */
        revert?: (status: string) => void | PromiseLike<void>;
        deferCancellation?: boolean;
        precancel?: () => void | PromiseLike<void>;
    }
    interface ContractController {
        canceled: boolean;
        confirmCancellation: () => Promise<void>;
    }
    class Contract<T> extends Promise<T> {
        canceled: boolean;
        constructor(init: (resolve: (value?: T | PromiseLike<T>) => Promise<void>, reject: (reason?: any) => Promise<void>, controller: ContractController) => void, options?: ContractOptionBag);
    }
    class AsyncContext<T> {
        constructor(callback: (context: AsyncContext<T>) => any, options?: ContractOptionBag);
        queue<U>(callback?: () => U | PromiseLike<U>, options?: ContractOptionBag): AsyncQueueItem<U>;
        feed(): AsyncFeed<T>;
        canceled: boolean;
        resolve(value?: T): Promise<void>;
        reject(error?: any): Promise<void>;
        cancel(): Promise<void>;
    }
    interface AsyncQueueConstructionOptionBag extends ContractOptionBag {
        context: AsyncContext<any>;
    }
    interface AsyncQueueOptionBag extends ContractOptionBag {
        behaviorOnCancellation?: string;
    }
    class AsyncQueueItem<T> extends Contract<T> {
        context: AsyncContext<any>;
        constructor(init: (resolve: (value?: T | PromiseLike<T>) => void, reject: (reason?: any) => void) => void, options: AsyncQueueConstructionOptionBag);
        queue<U>(onfulfilled?: (value: T) => U | PromiseLike<U>, options?: AsyncQueueOptionBag): AsyncQueueItem<U>;
        then<U>(onfulfilled?: (value: T) => U | PromiseLike<U>, onrejected?: (error: any) => U | PromiseLike<U>, options?: AsyncQueueOptionBag): AsyncQueueItem<U>;
        catch<U>(onrejected?: (error: any) => U | PromiseLike<U>, options?: ContractOptionBag): AsyncQueueItem<{}>;
    }
    class AsyncFeed<T> extends Contract<T> {
        constructor(init: (resolve: (value?: T | PromiseLike<T>) => Promise<void>, reject: (reason?: any) => Promise<void>, controller: ContractController) => void, options?: ContractOptionBag);
        cancel(): Promise<void>;
    }
}

export default AsyncChainer;
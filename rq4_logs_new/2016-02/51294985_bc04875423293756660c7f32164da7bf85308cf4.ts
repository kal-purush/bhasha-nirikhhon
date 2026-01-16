export declare type PromisedStateUpdater<T> = (state: T) => T | Promise<T>;
declare class PromisedState<T> {
    private state;
    private updaterQueue;
    constructor(initial: T);
    update(updater: PromisedStateUpdater<T>): Promise<T>;
    value(): Promise<T>;
}
export { PromisedState };
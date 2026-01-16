import AsyncChainer from "../built/async-context";
export { AsyncChainer }
let { AsyncFeed } = AsyncChainer;

export function waitFor(millisecond: number) {
    return new AsyncFeed((resolve, reject) => {
        setTimeout(() => resolve(), millisecond)
    })
} 

export function waitEvent<T extends Event>(element: EventTarget, eventName: string) {
    let callback: (evt: T) => void;
    return new AsyncFeed<void>((resolve, reject) => {
        callback = () => resolve();
        element.addEventListener(eventName, callback);
    }, { 
        revert: () => element.removeEventListener(eventName, callback)
    });
}

export function subscribeEvent<T extends Event>(element: EventTarget, eventName: string, listener: () => any) {
    let callback = (evt: T) => listener.call(element, evt, feed);
    var feed = new AsyncFeed<void>((resolve, reject) => {
        element.addEventListener(eventName, callback);
    }, {
        revert: () => element.removeEventListener(eventName, callback)
    });
    return feed;
}
import * as Obs from '../index.d.ts';

export function observe<T>(object?: any) {
    return Observable<T>(object);
}

var Observable = <T>(val: T): Obs.Observable<T> => {
    var value = val;
    var subscribers: Obs.Subscriber<T>[] = [];
    
    var getset: any;
    
    getset = (newValue?: T) => {
        if (newValue === undefined) return value;

        value = newValue;
        subscribers.forEach(fn => fn(newValue));
    }

    getset.subscribe = (fn: (newValue: T) => void) => {
        if (typeof fn !== 'function') throw new Error('Subscriber is not a function');

        subscribers.push(fn);
    }
    
    getset.removeSubscribers = () => this.subscribers = [];

    return getset;
} 
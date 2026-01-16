import {Observable} from "./Observable";
import Pipe from "./inputs/Pipe";
import {Observer} from "./Observer";

export default class Stream<T> {

    private input: Observable<T>;

    constructor(input: Observable<T>) {
        this.input = input;
    }

    forEach(cb): Stream<T> {
        let pipe = new Pipe<T>();
        this.input.subscribe({
            next: (item) => {
                cb(item);
                pipe.next(item);
            },
            throw: error => pipe.throw(error),
            return: () => pipe.return()
        });
        return new Stream<T>(pipe);
    }

    take(n: number): Stream<T> {
        let i = 0;
        let pipe = new Pipe<T>();
        let sub = this.input.subscribe({
            next: (item) => {
                i++;
                if(i > n) {
                    sub.unsubscribe();
                } else {
                    pipe.next(item);
                }
            },
            throw: error => pipe.throw(error),
            return: () => pipe.return()
        });
        return new Stream<T>(pipe);
    }

    subscribe(observer: Observer<T>): Stream<T> {
        this.input.subscribe(observer);
        return this;
    }

    return(cb): Stream<T> {
        this.input.subscribe({
            next: item => {},
            throw: error => {},
            return: cb
        });
        return this;
    }
}
import {Observer} from "./Observer";
import {Observable} from "./Observable";

export default class Range implements Observable {

    private from: Observable;
    private to: Observable;

    constructor(from: Observable, to: Observable) {
        this.from = from;
        this.to = to;
    }

    subscribe(observer: Observer): void {
        let from, to;
        this.from.subscribe({
            next: (f) => {
                from = f[0];
                if(from !== undefined && to !== undefined) {
                    this.range(observer, from, to);
                }
            }
        });
        this.to.subscribe({
            next: (t) => {
                to = t[0];
                if(from !== undefined && to !== undefined) {
                    this.range(observer, from, to);
                }
            }
        });
    }

    private range(observer: Observer, from: number, to: number): void {
        for(let i = from; i <= to; i++) {
            observer.next(i);
        }
    }
}
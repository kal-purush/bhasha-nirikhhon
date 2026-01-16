import {Observer} from "./Observer";
import {Observable} from "./Observable";

export default class Apply implements Observable {

    private fn;
    private args: Observable[];

    constructor(fn, args: Observable[]) {
        this.fn = fn;
        this.args = args;
    }

    subscribe(observer: Observer): void {
        let results = {};
        this.args.forEach((arg, idx) => {
            arg.subscribe({
                next: item => {
                   results[idx] = item;
                   if(Object.keys(results).length === this.args.length) {
                       let ret = this.fn.apply(this.fn, Object.keys(results).map(key => results[key]));
                       ret.forEach(v => observer.next(v));
                       observer.return();
                   }
               },
               return: () => {},
               throw: () => {}
            });
        });
    }
}
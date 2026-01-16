import {Observer} from "./Observer";
import {Observable} from "./Observable";

export default class Add implements Observable {

    private left: Observable;
    private right: Observable;

    constructor(left: Observable, right: Observable) {
        this.left = left;
        this.right = right;
    }

    subscribe(observer: Observer): void {
        let left, right;
        this.left.subscribe({
            next: (l) => {
                left = l[0];
                if(left !== undefined && right !== undefined) {
                    observer.next(left + right);
                }
            }
        });
        this.right.subscribe({
            next: (r) => {
                right = r[0];
                if(left !== undefined && right !== undefined) {
                    observer.next(left + right);
                }
            }
        });
    }
}
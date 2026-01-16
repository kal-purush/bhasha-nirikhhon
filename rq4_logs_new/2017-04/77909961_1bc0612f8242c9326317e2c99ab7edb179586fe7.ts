import { Observable } from 'rxjs';
import { Subject } from 'rxjs/Subject';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';


/**
 * Base class for providing an observer to changing item.
 */
export abstract class AppObserver<T> {
    observer: Observable<T>;
    protected subject: Subject<T> = new Subject<T>();

    constructor() {
        this.observer = this.subject.asObservable();
    }

    /**
     * Set next item.
     */
    protected setSubject(subject: T): void {
        this.subject.next(subject);
    }
}

/**
 * Function interface for validating item within AppObserverArray<T> class.
 */
interface SubjectComparisonFn<T> {
    (subject: T): boolean;
}

/**
 * Base class for providing an observer to changing array of items.
 */
export abstract class AppObserverArray<T> {
    observer: Observable<Array<T>>;
    protected subjects: BehaviorSubject<Array<T>>;
    private dataStore: {
        data: Array<T>
    };

    constructor() {
        this.dataStore = {data: []};
        this.subjects = <BehaviorSubject<Array<T>>>new BehaviorSubject([]);
        this.observer = this.subjects.asObservable();
    }

    /**
     * Remove all items.
     */
    removeAllSubjects(): void {
        this.dataStore.data = [];
        this.subjects.next(Object.assign({}, this.dataStore).data);
    }

    /**
     * Return number of items available in the observable.
     */
    get observerArrayLength(): number {
        return this.dataStore.data.length;
    }

    /**
     * Append new item.
     */
    protected addSubject(subject: T): void {
        this.dataStore.data.push(subject);
        this.subjects.next(Object.assign({}, this.dataStore).data);
    }

    /**
     * Remove item(s) from array. The caller must provide comparison implementation
     * for the item removal check.
     */
    protected removeSubject(validatorFn: SubjectComparisonFn<T>): void {
        this.dataStore.data.forEach((t, i) => {
            if (validatorFn(t)) { this.dataStore.data.splice(i, 1); }
        });
        this.subjects.next(Object.assign({}, this.dataStore).data);
    }
}
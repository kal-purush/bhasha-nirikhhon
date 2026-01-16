import { Observable } from 'rxjs';
import { ModelData, ModelReference } from './dataTypes';
import { Plump } from './plump';

export class PlumpObservable<T extends ModelData> extends Observable<T> {

  constructor(public plump: Plump, observable) {
    super();
    this.source = observable;
  }

  lift(operator) {
    const observable = new PlumpObservable(this.plump, this);
    observable.operator = operator;
    return observable;
  }

  inflateRelationship(relName: string) {
    return Observable.create(subscriber => {
      // because we're in an arrow function `this` is from the outer scope.
      let source = this;

      // save our inner subscription
      let subscription = source.subscribe(value => {
        // important: catch errors from user-provided callbacks
        try {
          if (value && value.relationships && value.relationships[relName]) {
            Observable.combineLatest(
              value.relationships[relName].map((v: ModelReference) => this.plump.find(v).asObservable())
            ).subscribe((v) => subscriber.next(v));
          } else {
            subscriber.next([]);
          }
        } catch (err) {
          subscriber.error(err);
        }
      },
      // be sure to handle errors and completions as appropriate and
      // send them along
      err => subscriber.error(err),
      () => subscriber.complete());

      // to return now
      return subscription;
    });
  }
}
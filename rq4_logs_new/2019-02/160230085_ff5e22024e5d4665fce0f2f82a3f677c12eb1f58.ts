import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import {merge, of} from 'rxjs';
import {filter, map} from 'rxjs/operators';

/**
 * Extends this class to be able to observe any key affectation
 */
class ObservableProxy {
  private changes$: Subject<[PropertyKey, any]> = new Subject<[PropertyKey, any]>();

  constructor() {
    return new Proxy(this, {
      set: (object, key, value) => {
        this.changes$.next([key, value]);
        object[key] = value;
        return true;
      }
    });
  }

  public $get<K extends keyof this>(key: K): Observable<this[K]> {
    const value: this[K] = this[key];

    const startWith$: Observable<this[K]> = of(value);

    return this.changes$.pipe(
      filter(([changedKey]) => {
        return changedKey === key;
      }),
      map(([changedKey, nextValue]) => nextValue)
    );
  }
}
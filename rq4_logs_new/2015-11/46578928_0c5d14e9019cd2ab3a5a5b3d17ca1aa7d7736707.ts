import { ChangeDetectorRef, Pipe, PipeTransform, PipeOnDestroy } from 'angular2/angular2'

import { toFirebase } from './utils/to_firebase'

@Pipe({
  name: 'onEvent', pure: false,
})

export class OnValuePipe implements PipeTransform, PipeOnDestroy {
  private _firebaseRef: Firebase;
  private _latestSnapshot: FirebaseDataSnapshot;

  constructor(private _changeDetectorRef: ChangeDetectorRef) {
  }

  onDestroy() {
    this._dispose();
  }

  transform(firebaseUrl: string | Firebase, args: string[] = []): FirebaseDataSnapshot {
    if (!firebaseUrl) {
      return null;
    }

    if (!this._firebaseRef || this._firebaseRef.toString() !== firebaseUrl) {
      this._dispose();
      this._subscribe(firebaseUrl);
    }

    return this._latestSnapshot;
  }

  private _subscribe(firebaseUrl: string | Firebase) {
    this._firebaseRef = toFirebase(firebaseUrl);
    this._firebaseRef.on('value', snapshot => {
      this._latestSnapshot = snapshot;
      this._changeDetectorRef.markForCheck();
    });
  }

  private _dispose() {
    if (this._firebaseRef) {
      this._firebaseRef.off();
      this._firebaseRef = null;
    }

    this._latestSnapshot = null;
  }
}
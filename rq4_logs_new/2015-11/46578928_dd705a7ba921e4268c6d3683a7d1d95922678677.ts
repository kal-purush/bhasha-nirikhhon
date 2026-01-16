import { Pipe, PipeTransform } from 'angular2/angular2'

class SnapshotWrapper {
  constructor(public $value: string | number | boolean, public $key: string | number) {
  }
}

@Pipe({
  name: 'toObject',
})

export class ToObjectPipe implements PipeTransform {
  transform(snapshot: FirebaseDataSnapshot, args: string[] = []): any {
    if (!snapshot || !snapshot.exists()) {
      return null;
    }

    let key = snapshot.key();
    let val = snapshot.val();

    if (typeof val == 'object') {
      return this._assignMeta(val, key);
    } else {
      return new SnapshotWrapper(val, key);
    }
  }

  private _assignMeta(object: Object, key: string | number) {
    object['$key'] = key;
    return object;
  }
}
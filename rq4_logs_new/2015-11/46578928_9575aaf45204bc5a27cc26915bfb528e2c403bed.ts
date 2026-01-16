import { ChangeDetectorRef, Pipe } from 'angular2/angular2'

import { TerminalPipeTransform } from './terminal_pipe_transform'
import { OnValuePipe } from './on_value_pipe'

@Pipe({
  name: 'hasChildren', pure: false,
})

export class HasChildrenPipe implements TerminalPipeTransform {
  private _onValuePipe: OnValuePipe;

  constructor(changeDetectorRef: ChangeDetectorRef) {
    this._onValuePipe = new OnValuePipe(changeDetectorRef, 'HasChildrenPipe');
  }

  transform(firebaseRef: string | FirebaseQuery, args: any[] = []): boolean {
    let snapshot = this._onValuePipe.transform(firebaseRef);
    return snapshot && snapshot.hasChildren();
  }

  onDestroy() {
    this._onValuePipe.onDestroy();
  }
}
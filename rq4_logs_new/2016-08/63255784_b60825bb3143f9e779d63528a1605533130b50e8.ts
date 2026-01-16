import {
  Injectable,
  Inject,
  OpaqueToken
} from '@angular/core';
import {
  CourseItem,
  AppState,
  IAppAction
} from '../model/index';


export let ITEM = new OpaqueToken('item');

@Injectable()
export class ToogleItemAction implements IAppAction {

  constructor(@Inject(ITEM) private item: CourseItem) {}

  createNewState(curentState: AppState): AppState {

    let newState = curentState.cloneState();

    newState.items.forEach( (aItem) => {
      if (aItem.title === this.item.title) {
        aItem.selected = !aItem.selected;
      }
    });

    return newState;
  }

}
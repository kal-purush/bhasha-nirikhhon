import { IAppAction, AppStateService} from '../model/app-state.service';
import { AppState } from '../model/app-state';
import { Injectable } from '@angular/core';
import { ItemsService } from '../model/items.service';
import { CourseItemsLoadedAction, ITEMS } from './course-items-loaded-action';


@Injectable()
export class LoadCourseItemsAction implements IAppAction {

  constructor(private itemsService: ItemsService, private appState: AppStateService) {}

  createNewState(curentState: AppState): AppState {

    let newState = curentState.cloneState();
    newState.items = [];

    let p = this.itemsService.loadItems();
    p.then( (items) => {
      this.appState.dispatchAction(CourseItemsLoadedAction, [{provide: ITEMS, useValue: items }]);
    });
    p.catch( (error) => {

    });

    return newState;
  }
}
import { Observable } from 'rxjs/Observable';
import { scan } from 'rxjs/operators';
import { uiActionType } from './ui.store';

export class UpdateEditTab {
  constructor(public index: number) {}
}

export type editTabUiActionType = UpdateEditTab;

export const editTabUiReducer$ = (
  init: number,
  action$: Observable<uiActionType>
): Observable<number> => {
  return action$.pipe(
    scan((state: any, action: uiActionType) => {
      if (action instanceof UpdateEditTab) {
        return handleUpdate(action);
      }
      return state;
    }, init)
  );
};

const handleUpdate = (action: UpdateEditTab): number => {
  return action.index;
};
import { IQuery } from '@autoschedule/queries-fn';
import { Observable } from 'rxjs/Observable';
import { scan } from 'rxjs/operators';
import { uiActionType } from './ui.store';

export class EditQueryAction {
  constructor(public query: IQuery) {}
}

export class CloseEditAction {
  constructor() {}
}

export type editUiActionType = EditQueryAction | CloseEditAction;

export interface EditUI {
  query: IQuery | false;
}

export const editUiReducer$ = (
  init: EditUI,
  action$: Observable<uiActionType>
): Observable<EditUI> => {
  return action$.pipe(
    scan((state: any, action: uiActionType) => {
      if (action instanceof EditQueryAction) {
        return handleEditQuery(action);
      }
      if (action instanceof CloseEditAction) {
        return handleClose();
      }
      return state;
    }, init)
  );
};

const handleEditQuery = (action: EditQueryAction): EditUI => {
  return {
    query: action.query
  };
};

const handleClose = (): EditUI => {
  return {
    query: false,
  };
}
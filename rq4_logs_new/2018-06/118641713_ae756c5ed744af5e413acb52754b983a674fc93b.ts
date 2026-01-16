import { IQuery } from '@autoschedule/queries-fn';
import { ICoreState } from '@scheduler-tester/core-state/core.state';
import { suiteToNewQuery } from '@scheduler-tester/core-state/suites.reducer';
import { UIState } from '@scheduler-tester/core-state/ui.state';
import { actionType } from './core.store';

export class AddQuery {
  constructor() {}
}

export type globalUiActionType = AddQuery;

export const globalUiReducer$ = ([state, action]: [ICoreState, actionType]): ICoreState => {
  if (action instanceof AddQuery) {
    return handleUpdate(state);
  }
  return state;
};

const handleUpdate = (state: ICoreState): ICoreState => {
  var newQuery: IQuery | false = false;
  console.log(state.onTestbenchQueries);
  const suiteState = {
    ...state,
    suites: state.suites.map((s, i) => {
      if (i !== state.onTestbenchQueries) {
        return s;
      }
      newQuery = suiteToNewQuery(s);
      return [...s, newQuery];
    }),
  };
  console.log(newQuery, suiteState);
  const uiState: UIState = {
    editTab: 0,
    edit: {
      query: newQuery,
    },
  };
  return {
    ...suiteState,
    ui: uiState,
  };
};
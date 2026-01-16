import { initialState, MainState } from '../state/main-state';
import { EVENT_FROM_EFFECT, INCREMENT } from '../actions/actions';
import { ActionReducer, Action } from '@ngrx/store';

export const mainReducer: ActionReducer<MainState> =
  (state = initialState, action: Action) => {
    switch (action.type) {
      case INCREMENT: {
        return {
          ...state,
          counter: state.counter + 1
        };
      }

      case EVENT_FROM_EFFECT: {
        return {
          ...state,
          counter: state.counter + 4
        };
      }

      default: {
        return state;
      }
    }
  }
;
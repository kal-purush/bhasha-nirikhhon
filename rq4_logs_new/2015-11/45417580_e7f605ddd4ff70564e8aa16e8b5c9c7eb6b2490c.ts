import * as Redux from 'redux';
import { handleActions, Action } from 'redux-actions';

import { Activity } from '../entites';
import { ADD_ACTIVITY, LOAD_ACTIVITIES } from '../action-types';

const activityReducer = handleActions<Activity[]>({
  [LOAD_ACTIVITIES]: (state: Activity[], action: Action): Activity[] => {
    return action.payload;
  },

  [ADD_ACTIVITY]: (state: Activity[], action: Action): Activity[] => {
    const activities = state;

    return [
      {
        id: action.payload.id,
        title: action.payload.title,
        timesOccurred: action.payload.timesOccurred,
        timeOfOccurence: action.payload.timeOfOccurence,
      },
      ...activities,
    ];
  },
}, []);

const reducers = Redux.combineReducers({
    activityReducer
});

export default reducers;
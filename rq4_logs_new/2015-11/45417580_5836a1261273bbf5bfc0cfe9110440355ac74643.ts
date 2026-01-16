import { Activity } from '../entites';
import { ADD_ACTIVITY, MARK_ACTIVITY_AS_RESOLVED } from '../action-types';
import initialState from '../initial-state';
import { createReducer } from '../utils';

const activities = {
  [ADD_ACTIVITY]: (state: Activity[], action): Activity[] => {
    return [
      ...state,
      {
        id: action.payload.id,
        title: action.payload.title,
        timesOccurred: action.payload.timesOccurred,
        timeOfOccurence: action.payload.timeOfOccurence,
        resolved: false,
      },
    ];
  },
  [MARK_ACTIVITY_AS_RESOLVED]: (state: Activity[], action): Activity[] => {
    return state.map(activity =>
      activity.id === action.activity.id
          ? Object.assign({}, activity, { resolved: !activity.resolved })
          : activity
    );
  },
};

export default createReducer(activities, initialState.activities);
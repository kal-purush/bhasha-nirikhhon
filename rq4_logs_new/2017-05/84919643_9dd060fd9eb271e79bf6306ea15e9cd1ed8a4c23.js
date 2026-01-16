import { RECIEVE_COLORS, FETCH_COLORS, FETCH_EVENTS, RECIEVE_EVENTS } from '../actions';

const fetchColorsReducer = (state = {}, action) => {
  switch (action.type) {
    case FETCH_COLORS:

      return Object.assign({}, state, { url: action.payload });
    case RECIEVE_COLORS:

      return Object.assign({}, state, { colors: action.payload.colorsArray });
    default:
      return state;
  }
};

const fetchEventsReducer = (state = {}, action) => {
  switch (action.type) {
    case FETCH_EVENTS:

      return Object.assign({}, state, { url: action.payload });
    case RECIEVE_EVENTS:
      // console.log(action.payload);
      return Object.assign({}, state, { events: action.payload });
    default:
      return state;
  }
};

export { fetchColorsReducer, fetchEventsReducer };
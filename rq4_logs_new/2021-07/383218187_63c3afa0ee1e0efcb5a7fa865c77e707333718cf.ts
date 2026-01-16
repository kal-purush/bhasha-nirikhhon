import { listOfLodgings } from "../actions";
import { ActionTypes } from "../actions/types";

export const lodgingUser = (state = [], action) => {
  switch (action.type) {
    case ActionTypes.lodgingUser:
      return action.payload;
    default:
      return state;
  }
};
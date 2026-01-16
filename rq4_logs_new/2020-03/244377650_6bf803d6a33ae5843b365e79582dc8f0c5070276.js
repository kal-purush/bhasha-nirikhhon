import { ADD_ORDER } from "./types";

export const addOrder = id => dispatch => {
  dispatch({
    type: ADD_ORDER,
    payload: id
  });
};
import { BulkSchedulerState } from "./interfaces";
import { ReduxAction } from "../../interfaces";
let initialState: BulkSchedulerState = {form: {}};

interface Handler {
  (state: BulkSchedulerState, action: ReduxAction<any>): BulkSchedulerState;
}

export function BulkSchedulerReducer (state: BulkSchedulerState = initialState,
                                     action: ReduxAction<any>): BulkSchedulerState {
  return ({
    SELECT_REGIMEN
  }[action.type] || NONE)(state, action);
};

function NONE(s: BulkSchedulerState, a: ReduxAction<any>) {
  return s;
};

function SELECT_REGIMEN(state: BulkSchedulerState,
                        action: ReduxAction<any>) {
  console.log("IT WORKS!");
  return state;
};
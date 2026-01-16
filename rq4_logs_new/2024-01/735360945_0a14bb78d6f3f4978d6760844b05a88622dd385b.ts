import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { RequestState, RequestTypes } from "../../utils/types";

type ReqSliceType = {
  reqType?: RequestTypes;
  requestState: RequestState;
};

const initialState: ReqSliceType = {
  requestState: RequestState.None,
};

const requestSlice = createSlice({
  name: "req",
  initialState,
  reducers: {
    setState(state, action: PayloadAction<ReqSliceType>) {
      state.requestState = action.payload.requestState;
      state.reqType = action.payload.reqType;
    },
  },
});

export const ReqActions = requestSlice.actions;
export const ReqReducer = requestSlice.reducer;

export type ChangeReqState = ReturnType<typeof ReqActions.setState>;
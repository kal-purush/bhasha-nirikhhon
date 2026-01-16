import { ActionWithPayload } from "./reducer";
import * as TYPES from "./actionTypes";
import { TLineDocument, TTimetableDocument } from "../firestore/DBCtrler.types";
import { SetLinePayload, SetStationsPayload, SetTrainPayload } from "./payload.type";
import { User } from "firebase/auth";

export const setLine = (id: string, line: TLineDocument): ActionWithPayload<SetLinePayload> => ({
  type: TYPES.SET_LINE,
  payload: {
    id: id,
    data: line
  }
});

export const setTrain = (id: string, data: TTimetableDocument): ActionWithPayload<SetTrainPayload> => ({
  type: TYPES.SET_LINE,
  payload: {
    id: id,
    data: data
  }
});

export const setStations = (map: SetStationsPayload): ActionWithPayload<SetStationsPayload> => ({
  type: TYPES.SET_LINE,
  payload: map
});

export const setCurrentUserAction = (user: User | null): ActionWithPayload<User | null> => ({
  type: TYPES.SET_USER,
  payload: user
});
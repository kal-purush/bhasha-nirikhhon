import ActionType, { Action } from "../actions";
import httpClient from "../../utils/http/http-client";
import { takeEvery, put } from "redux-saga/effects";
import { Token } from "../../index.d";
import { router } from "../../router";

export function* login(action: Action) {
  try {
    yield put({ type: ActionType.START_PROGRESS_BAR });
    const token: Token = yield httpClient.post<Token>("oauth/token", {
      grant_type: "password",
      client_id: "2",
      client_secret: "kpdNG8tz91xK4mfQ9YPZhbi3oRqmeJypMPMiDE3G",
      username: action.payload.username,
      password: action.payload.password
    });
    yield put({ type: ActionType.STOP_PROGRESS_BAR });
    if (token) {
      router.navigate("dashboard");
    }
  } catch (e) {
    console.log(e);
  }
}

function* me() {
  try {
    const me = yield httpClient.get("api/user");
    if (me) {
      console.log(me);
    }
  } catch (e) {
    console.log(e);
  }
}
/*
   Starts fetchUser on each dispatched `USER_FETCH_REQUESTED` action.
   Allows concurrent fetches of user.
 */
export function* mySaga() {
  yield takeEvery("login", login);
  yield takeEvery(ActionType.ME, me);
}
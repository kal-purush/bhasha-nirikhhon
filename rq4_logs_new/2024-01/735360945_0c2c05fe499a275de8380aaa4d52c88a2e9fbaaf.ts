import { ofType } from "redux-observable";
import { AuthActions } from "../slices/auth.slice";
import { EMPTY, catchError, mergeMap, of } from "rxjs";
import { API } from "../../api/API";
import { toast } from "react-toastify";
import { Epic } from "./epic";
import { AuthState } from "../../utils/types";
import { showFailToastMessage } from "../../main";

export const loginEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(AuthActions.login.type),
    mergeMap((action) => {
      return API.login(action.payload.email!, action.payload.password!).pipe(
        mergeMap((res) => EMPTY),
        catchError(() => {
          showFailToastMessage("Invalid username or password")
          return of(AuthActions.changeAuthState(AuthState.Unauthenticated));
        })
      );
    })
  );

  export const signupEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(AuthActions.signup.type),
    mergeMap((action) => {
      return API.login(action.payload.email!, action.payload.password!).pipe(
        mergeMap((res) => EMPTY),
        catchError(() => {
          toast.info("HI");
          return EMPTY;
        })
      );
    })
  );
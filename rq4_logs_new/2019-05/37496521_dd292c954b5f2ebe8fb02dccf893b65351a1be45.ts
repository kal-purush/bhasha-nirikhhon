/**
 * @module epics
 */
import { ofType } from "redux-observable";
import { ActionsObservable } from "redux-observable";
import { kernelspecs, ServerConfig } from "rx-jupyter";
import { empty, of } from "rxjs";
import { catchError, map, mergeMap } from "rxjs/operators";

import * as actions from "@nteract/actions";
import * as selectors from "@nteract/selectors";
import { KernelspecProps } from "@nteract/types";

export const fetchBookstoreValidationEpic = (
  action$: ActionsObservable<actions.FetchBookstoreValidation>,
  state$: any
) =>
  action$.pipe(
    ofType(actions.FETCH_BOOKSTORE_VALIDATION),
    map(() => actions.fetchBookstoreValidationSuccess()),
    catchError((xhrError: any) =>
      of(
        actions.fetchBookstoreValidationFailed({
          error: xhrError
        })
      )
    )
  );
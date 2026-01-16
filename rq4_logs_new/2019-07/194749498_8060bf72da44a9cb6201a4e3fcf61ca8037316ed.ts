import { Action } from 'redux';
import { ThunkDispatch, ThunkAction } from 'redux-thunk';

import { FetchError } from './action-type';

export const createThunkAction = <A extends Action, T extends any, S>(
  fn: Promise<T>,
  resolve: any,
  reject: any
): ThunkAction<Promise<void>, S, {}, A> => async (
  dispatch: ThunkDispatch<S, {}, A>
): Promise<void> => {
  fn.then(response => {
    if (response.ok) {
      return dispatch(resolve(response));
    }
    throw new Error(response.status);
  }).catch((error: FetchError) => {
    return dispatch(reject(error));
  });
};
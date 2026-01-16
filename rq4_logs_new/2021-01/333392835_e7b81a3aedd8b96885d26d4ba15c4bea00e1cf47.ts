import { ThunkAction } from 'redux-thunk';
import { Action, Dispatch } from 'redux';
import { RootState } from '../reducers';

export const FETCH_PRODUCT_REQUEST = 'FETCH_PRODUCT_REQUEST';
export const FETCH_PRODUCT_SUCCESS = 'FETCH_PRODUCT_SUCCESS';
export const FETCH_PRODUCT_FAILURE = 'FETCH_PRODUCT_FAILURE';

export const fetchProducts = (): ThunkAction<void, RootState, unknown, Action<string>> => async (
  dispatch: Dispatch
) => {
  dispatch({ type: FETCH_PRODUCT_REQUEST });
  const productURL = '/product_list.json';
  try {
    await fetch(productURL)
      .then((res) => res.json())
      .then((data) => {
        dispatch({
          type: FETCH_PRODUCT_SUCCESS,
          payload: data
        });
      });
  } catch (e) {
    dispatch({
      type: FETCH_PRODUCT_FAILURE,
      payload: e
    });
  }
};
import {DispatchType, ActionType} from 'types';
import IApiAction from 'types/IApiAction';
import * as apiActionTypes from 'constants/action-types/apiActions';

const URI = 'https://api.api-ninjas.com/v1'

const defaultOnStart =
  () =>
  (dispatch: DispatchType): void => {
    dispatch({
      type: apiActionTypes.API_REQUEST_START,
      payload: {loading: true},
    });
  };
const defaultOnSuccess =
  () =>
  (dispatch: DispatchType): void => {
    dispatch({
      type: apiActionTypes.API_REQUEST_SUCCESS,
      payload: {loading: false},
    });
  };
const defaultOnFailure =
  () =>
  (dispatch: DispatchType): void => {
    dispatch({
      type: apiActionTypes.API_REQUEST_FAILURE,
      payload: {loading: false},
    });
  };
const defaultOnEnd =
  () =>
  (dispatch: DispatchType): void => {
    dispatch({type: apiActionTypes.API_REQUEST_END, payload: {loading: false}});
  };

export default ({
  url,
  baseUrl,
  method,
  data,
  resType,
  params: _params,
  options,
  headers,
  onStart,
  onSuccess,
  onFailure,
  onEnd,
}: IApiAction): ActionType => {
  const params = {} as Record<string, string>;

  if (_params && typeof _params === 'object') {
    Object.keys(_params).forEach((key: string) => {
      params[key] =
        typeof _params[key] === 'string' ? _params[key].trim() : _params[key];
    });
  }

  return {
    type: apiActionTypes.API_REQUEST,
    payload: {
      url: `${URI}${String(url || '')}`,
      method,
      options,
      headers,
      data,
      resType,
      params,
      onStart: onStart || defaultOnStart,
      onSuccess: onSuccess || defaultOnSuccess,
      onFailure: onFailure || defaultOnFailure,
      onEnd: onEnd || defaultOnEnd,
    },
  };
};
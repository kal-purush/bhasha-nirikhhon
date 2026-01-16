import { ThunkAction, ThunkDispatch } from 'redux-thunk';
import { ActionCreator } from 'redux';

import { SET_TIME_SERIES, SET_TIME_SERIES_ERROR } from './constants';
import { ActionSet } from './defaults';
import { TimeSeries, fetchTimeSeries } from '../util';

export type TimeSeriesAction = ActionSet<TimeSeries> | ActionSet<string>;

export const setTimeSeries: ActionCreator<TimeSeriesAction> = (
  payload: TimeSeries
): TimeSeriesAction => ({ type: SET_TIME_SERIES, payload });

export const setTimeSeriesError: ActionCreator<TimeSeriesAction> = (
  payload: string
): TimeSeriesAction => ({ type: SET_TIME_SERIES_ERROR, payload });

export const getTimeSeries: ActionCreator<
  ThunkAction<Promise<void>, {}, {}, TimeSeriesAction>
> = (
  companySymbol: string
): ThunkAction<Promise<void>, {}, {}, TimeSeriesAction> => async (
  dispatch: ThunkDispatch<{}, {}, TimeSeriesAction>
) => {
  fetchTimeSeries(companySymbol)
    .then(response => {
      dispatch(setTimeSeries(response));
    })
    .catch(error => {
      dispatch(setTimeSeriesError(error));
    });
};
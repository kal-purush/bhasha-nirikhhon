import { ThunkAction, ThunkDispatch } from 'redux-thunk';
import { ActionCreator } from 'redux';

import { SET_KEY_STATS, SET_KEY_STATS_ERROR } from './constants';
import { ActionSet } from './defaults';
import { KeyStats, fetchKeyStats } from '../util';

export type KeyStatsAction = ActionSet<KeyStats> | ActionSet<string>;

export const setKeyStats: ActionCreator<KeyStatsAction> = (
  payload: KeyStats
): KeyStatsAction => ({ type: SET_KEY_STATS, payload });

export const setKeyStatsError: ActionCreator<KeyStatsAction> = (
  payload: string
): KeyStatsAction => ({ type: SET_KEY_STATS_ERROR, payload });

export const getKeyStats: ActionCreator<
  ThunkAction<Promise<void>, {}, {}, KeyStatsAction>
> = (
  companySymbol: string
): ThunkAction<Promise<void>, {}, {}, KeyStatsAction> => async (
  dispatch: ThunkDispatch<{}, {}, KeyStatsAction>
) => {
  fetchKeyStats(companySymbol)
    .then(response => {
      dispatch(setKeyStats(response));
    })
    .catch(error => {
      dispatch(setKeyStatsError(error));
    });
};
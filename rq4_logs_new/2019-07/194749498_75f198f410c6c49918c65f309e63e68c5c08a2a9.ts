import { Reducer } from 'redux';
import { KeyStatsAction } from '../actions/keystats-actions';
import { KeyStats } from '../util/';
import {
  SET_KEY_STATS,
  GET_KEY_STATS,
  SET_KEY_STATS_ERROR,
} from '../actions/constants';

export interface KeyStatsState {
  keyStats: KeyStats;
  error: string | undefined;
  isLoading: boolean;
}

export const defaultState: KeyStatsState = {
  keyStats: undefined,
  error: undefined,
  isLoading: false,
};

export const keyStatsReducer: Reducer<KeyStatsState, KeyStatsAction> = (
  state: any = defaultState,
  { type, payload }: KeyStatsAction
): KeyStatsState => {
  switch (type) {
    case SET_KEY_STATS:
      return { ...state, keyStats: payload, isLoading: false };
    case SET_KEY_STATS_ERROR:
      return { ...state, error: payload, isLoading: false };
    case GET_KEY_STATS:
      return { ...state, isLoading: true };
    default:
      return state;
  }
};
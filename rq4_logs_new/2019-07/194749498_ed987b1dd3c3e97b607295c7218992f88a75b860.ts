import { EARNINGS_ACTIONS_TYPES } from './constants';
import { Earnings, fetchEarnings, MapObject } from '../util';
import {
  createAction,
  createThunkAction,
  ActionType,
  FetchError,
} from '../action-creators';

export type EarningsAction = ActionType<EARNINGS_ACTIONS_TYPES, Earnings>;

export const earningsActions = {
  setPayload: (type: EARNINGS_ACTIONS_TYPES, payload: Earnings) =>
    createAction({ type, payload }),
  setError: (type: EARNINGS_ACTIONS_TYPES, error: FetchError) =>
    createAction({ type, error }),
  setLoading: (type: EARNINGS_ACTIONS_TYPES) => ({ type }),
  getData: (companySymbol: string, parameters?: MapObject<string>) =>
    createThunkAction(
      fetchEarnings(companySymbol),
      earningsActions,
      EARNINGS_ACTIONS_TYPES
    ),
};
import { FAVORITES_ACTIONS_TYPES } from './constants';
import { Favorites, fetchFavorites, favoritesFilter } from '../util';
import {
  createAction,
  createThunkAction,
  ActionType,
  FetchError,
} from '../action-creators';

export type FavoritesAction = ActionType<FAVORITES_ACTIONS_TYPES, Favorites>;

const favoritesParameter = (symbols: string) => ({
  ...favoritesFilter,
  symbols,
  types: 'quote',
});

export const favoritesActions = {
  setPayload: (type: FAVORITES_ACTIONS_TYPES, payload: Favorites) =>
    createAction({ type, payload }),
  setError: (type: FAVORITES_ACTIONS_TYPES, error: FetchError) =>
    createAction({ type, error }),
  setLoading: (type: FAVORITES_ACTIONS_TYPES) => ({ type }),
  getData: (companySymbols: string) =>
    createThunkAction(
      fetchFavorites(favoritesParameter(companySymbols)),
      favoritesActions,
      FAVORITES_ACTIONS_TYPES
    ),
};
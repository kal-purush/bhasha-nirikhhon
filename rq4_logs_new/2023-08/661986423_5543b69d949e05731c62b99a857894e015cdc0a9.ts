import { AxiosInstance } from 'axios';
import { createAsyncThunk } from '@reduxjs/toolkit';
import { AppDispatch, State } from '../types/state.js';
import { TOffer, TFullOffer } from '../types/offers.js';

import {
  fetchOffer,
  fetchOffers,
  fetchFavorites,
  loadComments,
  setCommentsDataLoadingStatus,
  setFullOfferDataLoadingStatus,
  setOffersDataLoadingStatus,
  requireAuthorization
} from './action';

import { saveToken, dropToken } from '../services/token';
import { APIRoute, AuthorizationStatus } from '../const';
import { TAuthData } from '../types/auth-data';
import { TComment, TUser } from '../types/comments.js';

export const fetchOffersAction = createAsyncThunk<void, undefined, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'data/fetchOffers',
  async (_arg, { dispatch, extra: api }) => {
    dispatch(setOffersDataLoadingStatus(true));
    const { data } = await api.get<TOffer[]>(APIRoute.Offers);
    dispatch(setOffersDataLoadingStatus(false));
    dispatch(fetchOffers(data));
  },
);

export const fetchOfferAction = createAsyncThunk<void, string, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'OFFER/fetch',
  async (offerId, { dispatch, extra: api }) => {
    dispatch(setFullOfferDataLoadingStatus(true));
    const { data } = await api.get<TFullOffer>(`${APIRoute.Offers}/${offerId}`);
    dispatch(setFullOfferDataLoadingStatus(false));
    dispatch(fetchOffer(data));
  },
);

export const fetchCommentsOfferAction = createAsyncThunk<void, string, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'REVIEWS/fetch',
  async (offerId, { dispatch, extra: api }) => {
    dispatch(setCommentsDataLoadingStatus(true));
    const { data } = await api.get<TComment[]>(`${APIRoute.Comments}/${offerId}`);
    dispatch(setCommentsDataLoadingStatus(false));
    dispatch(loadComments(data));
  },
);

export const fetchFavoritesAction = createAsyncThunk<void, undefined, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'FAVORITES/fetch',
  async (_arg, { dispatch, extra: api }) => {
    const { data } = await api.get<TOffer[]>(APIRoute.Favorites);
    dispatch(fetchFavorites(data));
  },
);

export const checkAuthAction = createAsyncThunk<void, undefined, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'user/checkAuth',
  async (_arg, { dispatch, extra: api }) => {
    try {
      await api.get<TUser>(APIRoute.Login);
      dispatch(requireAuthorization(AuthorizationStatus.Auth));
    } catch {
      dispatch(requireAuthorization(AuthorizationStatus.NoAuth));
    }
  },
);

export const loginAction = createAsyncThunk<void, TAuthData, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'user/login',
  async ({ login: email, password }, { dispatch, extra: api }) => {
    const { data: { token } } = await api.post<TUser>(APIRoute.Login, { email, password });
    saveToken(token);
    dispatch(requireAuthorization(AuthorizationStatus.Auth));
  },
);

export const logoutAction = createAsyncThunk<void, undefined, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
}>(
  'user/logout',
  async (_arg, { dispatch, extra: api }) => {
    await api.delete(APIRoute.Logout);
    dropToken();
    dispatch(requireAuthorization(AuthorizationStatus.NoAuth));
  },
);
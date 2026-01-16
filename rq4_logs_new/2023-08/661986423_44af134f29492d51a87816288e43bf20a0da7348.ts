import { createAction } from '@reduxjs/toolkit';
import { TFullOffer } from '../types/offers';

export const setActiveCity = createAction<string>('OFFERS/setActiveCity');

export const fetchOffers = createAction('OFFERS/fetch');

export const fetchOffer = createAction<TFullOffer['id']>('OFFER/fetch');

export const fetchComments = createAction('data/getComments', (comments: Comment[] | null) => ({ payload: comments }));

export const fetchNearPlacesOffers = createAction<TFullOffer['id']>('OFFER/fetch');

export const dropOffer = createAction('OFFER/drop');

export const fetchFavorites = createAction('FAVORITES/fetch');

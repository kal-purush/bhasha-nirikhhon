import { createReducer } from '@reduxjs/toolkit';
import { offers } from '../mocks/offers';
import { changeCity, getOffers, selectOffer } from './action';
import { CITIES } from '../constants';
import { Offer } from '../types/offer';

type InitialState = {
  city: string;
  offers: Offer[];
  offersByCity: Offer[];
  selectedOfferId: string;
};

const initialState : InitialState = {
  city: CITIES[0],
  offers: offers,
  offersByCity: offers.filter((offer) => (
    offer.city.name === CITIES[0]
  )),
  selectedOfferId:''
};

const reducer = createReducer(initialState, (builder) => {
  builder
    .addCase(changeCity, (state, action) => {
      const {city} = action.payload;
      state.city = city;
    })
    .addCase(getOffers, (state) => {
      state.offersByCity = state.offers.filter((offer) => (
        offer.city.name === state.city
      ));
    })
    .addCase(selectOffer, (state, action) => {
      const {id} = action.payload;
      state.selectedOfferId = id;
    });
});

export { reducer };
import { FetchDetailHotel } from './../actions/index';
import { ActionTypes } from "../actions/types";

export interface cardsHotel{
  name: String,
  summary: String,
  type: String,
  accommodates: Number,
  beds: Number,
  bedrooms: Number,
  bathrooms: String,
  amenities: [String],
  price: String,
  image: String,
  address: String,
  score: Number,
}

export const detailReducer = (state: cardsHotel[] = [], action: FetchDetailHotel) => {
  switch (action.type) {
    case ActionTypes.detailHotel:
      return action.payload;
    default:
      return state;
  }
};
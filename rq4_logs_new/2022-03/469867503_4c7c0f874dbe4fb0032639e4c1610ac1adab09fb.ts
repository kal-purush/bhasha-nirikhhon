import {
  FETCH_CITIES_REQUEST,
  FETCH_CITIES_SUCCESS,
  FETCH_CITIES_ERROR,
  SELECTED_ADDRESS_CITY,
} from "./types";

export const FetchCitiesRequest = () => ({
  type: FETCH_CITIES_REQUEST,
});

export const FetchCitiesSuccess = (payload: any) => ({
  type: FETCH_CITIES_SUCCESS,
  payload,
});

export const FetchCitiesError = (payload: any) => ({
  type: FETCH_CITIES_ERROR,
});

export const SelectedAddressCity = (payload: string) => ({
  type: SELECTED_ADDRESS_CITY,
  payload: {
    selectedCity: payload,
  },
});
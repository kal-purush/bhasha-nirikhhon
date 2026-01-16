import { action } from 'typesafe-actions';
import { ActionTypes } from './constants';

export function getCountries() {
  return action(ActionTypes.GET_COUNTRIES);
}

export function getCountriesSuccess(payload: any) {
  return action(ActionTypes.GET_COUNTRIES_SUCCESS, payload);
}

export function getCountriesFailed(payload: any) {
  return action(ActionTypes.GET_COUNTRIES_FAILED, payload);
}

export function getStates() {
  return action(ActionTypes.GET_STATES);
}

export function getStatesSuccess(payload: any) {
  return action(ActionTypes.GET_STATES_SUCCESS, payload);
}

export function getStatesFailed(payload: any) {
  return action(ActionTypes.GET_STATES_FAILED, payload);
}

export function getCities() {
  return action(ActionTypes.GET_CITIES);
}

export function getCitiesSuccess(payload: any) {
  return action(ActionTypes.GET_CITIES_SUCCESS, payload);
}

export function getCitiesFailed(payload: any) {
  return action(ActionTypes.GET_CITIES_FAILED, payload);
}
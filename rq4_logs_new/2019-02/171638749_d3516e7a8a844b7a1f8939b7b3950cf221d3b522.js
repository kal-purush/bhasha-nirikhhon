import * as types from './actionTypes';

export function updateLocationSuccess(location) {
    return {type: types.UPDATE_LOCATION_SUCCESS, location};
}

export function loadLocation(location) {
    return {type: types.LOAD_LOCATION, location};
}
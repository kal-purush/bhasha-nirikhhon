import { Action } from '@ngrx/store';
import { Location } from '../../models/index';

export enum LocationActionsType { 
    GET_LOCATIONS = '[LOCATIONS] GET_LOCATIONS',
    ADD_LOCATION = '[LOCATIONS] ADD_LOCATION', 
    GET_LOCATIONS_SUCCESS = '[LOCATIONS] GET_LOCATIONS_SUCCESS',
    GET_LOCATIONS_ERROR = '[LOCATIONS] GET_LOCATIONS_ERROR',
}


export class GetLocations {
    public readonly type = LocationActionsType.GET_LOCATIONS;
}

export class AddLocation {
    public readonly type = LocationActionsType.ADD_LOCATION;
    constructor (public payload: Location) {
        
    }
}

export class GetLocationsSuccess {
    public readonly type = LocationActionsType.GET_LOCATIONS_SUCCESS;
    constructor (public payload: Location[]) {

    }
}

export class GetLocationsError {
    public readonly type = LocationActionsType.GET_LOCATIONS_ERROR;
    constructor (public payload: any | string) {

    }
}

export type LocationsActions = GetLocations | AddLocation | GetLocationsSuccess | GetLocationsError;

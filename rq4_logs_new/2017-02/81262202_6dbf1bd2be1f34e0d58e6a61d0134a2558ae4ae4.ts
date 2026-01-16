import { Action } from '@ngrx/store';
import { Album } from '../models/album';
import { type } from '../util';

export const ActionTypes = {
    FETCH_FAVOURITES:          type('[Album] Fetch Favourites'),
    LOAD_FAVOURITES:           type('[Album] Load Favourites'),
    ADD_FAVOURITE:             type('[Album] Add favourite'),
    REMOVE_FAVOURITE:          type('[Album] Remove favourite'),    
};

export class LoadFavouritesAction implements Action {
    type = ActionTypes.LOAD_FAVOURITES;

    constructor(public payload: Album[]) { }
}

export class FetchFavouritesAction implements Action {
    type = ActionTypes.FETCH_FAVOURITES;

    constructor(public payload: Album[]) { }
}

export class AddFavouriteAction implements Action {
    type = ActionTypes.ADD_FAVOURITE;

    constructor(public payload: Album) { }
}

export class RemoveFavouriteAction implements Action {
    type = ActionTypes.REMOVE_FAVOURITE;

    constructor(public payload: Album) { }
}

export type Actions
    = LoadFavouritesAction | FetchFavouritesAction | AddFavouriteAction | RemoveFavouriteAction ;
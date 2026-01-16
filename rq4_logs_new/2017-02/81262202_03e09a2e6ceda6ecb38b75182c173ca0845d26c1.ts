import { Action } from '@ngrx/store';
import { Album } from '../models/album';
import { Photo } from '../models/photo';
import { type } from '../util';

export const ActionTypes = {
    LOAD:               type('[Album] Load'), 
    FETCH_ALBUM_PHOTO:   type('[Photo] Fetch Album Photo'), 
};

export class LoadAction implements Action {
    type = ActionTypes.LOAD;

    constructor(public payload: Album[]) { }
}

export class FetchAlbumPhotoAction implements Action {
    type = ActionTypes.FETCH_ALBUM_PHOTO;

    constructor(public payload: Photo[]) { }
}

export type Actions
    = LoadAction | FetchAlbumPhotoAction;
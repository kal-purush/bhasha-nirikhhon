import { Coords } from 'google-map-react';
import { Moment } from 'moment';
import { Action } from 'redux';

import { SET_COORDS, SET_DATE, SET_IMAGE, SET_TYPE } from './types';

export interface AddStickerState {
    image: File;
    type: string; // placed | spotted
    date: Moment;
    coords: Coords;
}

const initialState: AddStickerState = {
    image: null,
    type: null,
    date: null,
    coords: null,
};

export default function addStickerReducer(state: AddStickerState, action: Action) {
    switch (action.type) {
        case SET_IMAGE:
            return { ...state, image: action.payload };
        case SET_TYPE:
            return { ...state, type: action.payload };
        case SET_DATE:
            return { ...state, date: action.payload };
        case SET_COORDS:
            return { ...state, coords: action.payload };
        default:
            return initialState;
    }
}
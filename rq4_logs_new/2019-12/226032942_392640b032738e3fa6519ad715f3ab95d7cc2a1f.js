import _ from 'lodash';
import {
    FETCH_POKEMON_DETAIL
} from './../actions/types';

let defaultState = {
    moves: [],
    types: []
}

export default (state=defaultState, action) => {
    switch(action.type){
        case FETCH_POKEMON_DETAIL:
            return action.payload;
        default:
            return state;
    }
}
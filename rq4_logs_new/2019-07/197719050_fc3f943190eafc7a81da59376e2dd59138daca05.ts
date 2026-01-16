import {ADD_POKEMON, IActionAddPlace} from './types';
import {IPokemon} from '../interfaces';

export const addPokemon = (pokemon: IPokemon): IActionAddPlace => {
  return {
    type: ADD_POKEMON,
    payload: pokemon,
  };
};
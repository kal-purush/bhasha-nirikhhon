import { ADD_TO_PLATE } from '../constants';

export const addItemToPlate = item => ({
  type: ADD_TO_PLATE,
  item
});
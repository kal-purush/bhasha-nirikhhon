import { createReducer } from 'typesafe-actions';
import produce from 'immer';
import { ADD_METHOD, CLEAR_METHOD, DELETE_METHOD, ADD_MATERIAL, CLEAR_MATERIAL, DELETE_MATERIAL } from '../actions';
import { Filter, FilterAction } from '../types';

const initailState: Filter = {
  method: [],
  material: [],
};

const filter = createReducer<Filter, FilterAction>(initailState, {
  [ADD_METHOD]: (state, action) =>
    produce(state, (draft) => {
      draft.method.push(action.payload.method);
    }),
  [DELETE_METHOD]: (state, action) => ({
    ...state,
    method: state.method.filter((method) => method !== action.payload.method),
  }),
  [CLEAR_METHOD]: (state, action) => ({
    ...state,
    method: [],
  }),
  [ADD_MATERIAL]: (state, action) =>
    produce(state, (draft) => {
      draft.material.push(action.payload.material);
    }),
  [DELETE_MATERIAL]: (state, action) => ({
    ...state,
    material: state.material.filter((material) => material !== action.payload.material),
  }),
  [CLEAR_MATERIAL]: (state, action) => ({
    ...state,
    material: [],
  }),
});

export default filter;
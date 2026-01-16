import * as types from '../actions/action-types.js';

const genres = (state = [], action) => {
  switch (action.type) {
    case types.ADD_GENRES:
      return state.concat(action.genres);
    default:
      return state
  }
}

export default genres;
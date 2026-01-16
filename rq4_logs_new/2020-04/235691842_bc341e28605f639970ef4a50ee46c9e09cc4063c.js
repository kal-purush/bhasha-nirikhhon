import * as SearchAPIUtil from '../utils/search_util';

export const RECEIVE_SEARCH_USER = 'RECEIVE_SEARCH_USER';

const receiveSearchUser = user => ({
  type: RECEIVE_SEARCH_USER,
  user
});

export const searchUser = user => dispatch => (
  SearchAPIUtil.searchUser(user).then(user => dispatch(receiveSearchUser(user)))
);
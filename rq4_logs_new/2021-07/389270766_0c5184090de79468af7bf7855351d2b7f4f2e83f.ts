import { createSelector } from 'reselect'

import { Selector, Users } from './ActionType';

const usersSelector = (state: Selector) => state.users;

export const getUserName = createSelector(
  [usersSelector],
  (state: Users) => state.username
)

export const getUserState = createSelector(
  [usersSelector],
  (state: Users) => state.isSignedIn
)
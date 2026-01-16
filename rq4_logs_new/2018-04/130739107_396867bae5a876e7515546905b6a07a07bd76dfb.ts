import { createSelector } from '@ngrx/store';

import { userState } from './../reducers/user.reducer';


export const getUser = createSelector(
  userState,
  (state) => state
);

export const getUserEmail = createSelector(
  getUser,
  (state) => state.email
);

export const getUserFirstName = createSelector(
  getUser,
  (state) => state.firstName
);

export const getUserLastName = createSelector(
  getUser,
  (state) => state.lastName
);

export const getUserPic = createSelector(
  getUser,
  (state) => state.pic
);
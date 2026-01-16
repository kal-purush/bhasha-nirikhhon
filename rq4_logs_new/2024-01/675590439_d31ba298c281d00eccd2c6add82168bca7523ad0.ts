import { createSelector } from '@reduxjs/toolkit';
import type { AccountState } from '../slices/account';
import type { State } from '../store';

const getAccount = (state: State): AccountState => (
  state.account
);

export const getUserId = createSelector(
  getAccount,
  account => account.userId,
);

export const getUsername = createSelector(
  getAccount,
  account => account.username,
);

export const getRole = createSelector(
  getAccount,
  account => account.role,
);

export const getLoggedIn = createSelector(
  getUserId,
  userId => userId !== undefined,
);

export const getIsClient = createSelector(
  getRole,
  role => role === 'client',
);

export const getIsEmployee = createSelector(
  getRole,
  role => role === 'employee',
);

export const getIsAdmin = createSelector(
  getRole,
  role => role === 'admin',
);
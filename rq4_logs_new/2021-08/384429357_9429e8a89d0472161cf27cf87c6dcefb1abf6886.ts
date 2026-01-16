import { tryAutoSignIn, autoSignIn, logout } from './authSlice';
import { Middleware } from 'redux'
import { RootState } from 'store';
import { AnyAction, isFulfilled } from '@reduxjs/toolkit';
import { Dispatch } from 'react';

const clearAuthLocalStorage = () => {
  localStorage.removeItem("token");
  localStorage.removeItem("expirationDate");
  localStorage.removeItem("userId");
}

let expireAuthTiemout: any;

const setAuthTimeout = (expirationDateISO: string, dispatch: Dispatch<AnyAction>) => {
  expireAuthTiemout = setTimeout(() => {
    dispatch(logout());
  }, new Date(expirationDateISO).getTime() - new Date().getTime())
}


export const authMiddleware: Middleware<{}, RootState> = ({ dispatch }) => (nextDispatch) => (action) => {
  if (logout.match(action)) {
    clearAuthLocalStorage()
    clearTimeout(expireAuthTiemout)
  }

  if (tryAutoSignIn.match(action)) {
    const token = localStorage.getItem('token');
    const expirationDateISO = localStorage.getItem("expirationDate");
    const userId = localStorage.getItem("userId");

    if (!token || !expirationDateISO || !userId) return clearTimeout(expireAuthTiemout);

    if (new Date(expirationDateISO) <= new Date()) return clearAuthLocalStorage();

    setAuthTimeout(expirationDateISO, dispatch);
    return nextDispatch(autoSignIn({ token, userId }))
  }

  if (action?.type.match(/^auth/) && isFulfilled(action)) {
    const { idToken, localId, expiresIn } = action.payload;
    const expirationDate = new Date(new Date().getTime() + parseInt(expiresIn, 10) * 1000).toISOString();

    setAuthTimeout(expirationDate, dispatch);
    localStorage.setItem('token', idToken);
    localStorage.setItem('expirationDate', expirationDate);
    localStorage.setItem('userId', localId);
  }

  return nextDispatch(action);
};
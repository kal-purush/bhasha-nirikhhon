import { clearStoredData } from './clearStoredData';
import { getStoredToken } from './getStoredToken';
import { getStoredUser } from './getStoredUser';
import { setStoredToken } from './setStoredToken';
import { setStoredUser } from './setStoredUser';

export const storeInteractor = {
  setToken: setStoredToken,
  setUser: setStoredUser,
  getToken: getStoredToken,
  getUser: getStoredUser,
  clearAll: clearStoredData,
  checkToken: getStoredToken() ? true : false,
  checkUser: getStoredUser() ? true : false,
};
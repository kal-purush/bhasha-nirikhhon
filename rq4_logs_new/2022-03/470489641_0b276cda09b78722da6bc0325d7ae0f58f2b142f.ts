import { IResponseData } from '../components/hooks/useProvideAuth';

export const getUserFromLocalStorage = () => {
  return localStorage.getItem(`${process.env.REACT_APP_LOCALSTORAGE_KEY}`);
};

export const setUserFromLocalStorage = (data: IResponseData) => {
  return localStorage.setItem(`${process.env.REACT_APP_LOCALSTORAGE_KEY}`, JSON.stringify(data));
};

export const removeUserFromLocalStorage = () => {
  localStorage.removeItem(`${process.env.REACT_APP_LOCALSTORAGE_KEY}`);
};
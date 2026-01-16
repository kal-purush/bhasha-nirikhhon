import axios, {AxiosInstance, InternalAxiosRequestConfig  } from 'axios';
import { BASE_URL, TIMEOUT } from '../consts';
import { getToken } from './token';
//import { StatusCodes } from 'http-status-codes';


// const StatusCodeMapping: Record<number, boolean> = {
//   [StatusCodes.BAD_REQUEST]: true,
//   [StatusCodes.UNAUTHORIZED]: true,
//   [StatusCodes.NOT_FOUND]: true
// };


export const createAPI = (): AxiosInstance => {
  const api = axios.create({
    baseURL: BASE_URL,
    timeout: TIMEOUT,
  });

  api.interceptors.request.use(
    (config: InternalAxiosRequestConfig ) => {
      const token = getToken();

      if (token && config.headers) {
        config.headers['x-token'] = token;
      }

      return config;
    },
  );


  return api;
};
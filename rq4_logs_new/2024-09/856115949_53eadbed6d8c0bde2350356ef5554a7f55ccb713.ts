import axios from "axios";
import { getCookieValue } from "hooks/getCookie";

const BASE_URL = process.env.REACT_APP_API_URL as string;

const baseAPI = (url: string, options?: any) => {
  const instance = axios.create({ baseURL: url, ...options });
  return instance;
};

const authAPI = (url: string, options?: any) => {
  const instance = axios.create({
    baseURL: url,
    ...options,
  });

  instance.interceptors.request.use(
    (config) => {
      const token = getCookieValue("access_token");
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  return instance;
};

export const baseInstance = baseAPI(BASE_URL);
export const authInstance = authAPI(BASE_URL);
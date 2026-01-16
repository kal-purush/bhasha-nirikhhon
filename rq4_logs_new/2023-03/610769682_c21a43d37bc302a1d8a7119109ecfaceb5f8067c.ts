import axios, { AxiosInstance, AxiosResponse, AxiosError } from "axios";
import store from "store";
import { actions, store as reduxStore } from "../store";

const { alert: alertAction } = actions;

const instance: AxiosInstance = axios.create({
  baseURL: process.env.REACT_APP_API_URL,
});

instance.interceptors.request.use(
  async (config) => {
    const token = await store.get("accessToken");

    if (typeof token !== "undefined") {
      config.headers.Authorization = `Bearer ${token}`;
      config.headers["Content-Type"] = "application/json";
    }
    return config;
  },
  (error: any) => {
    return Promise.reject(error);
  },
);

const successHandler = (response: AxiosResponse) => {
  return response;
};

const errorHandler = (error: AxiosError) => {
  const status = error?.response?.status;
  if (status === 401) {
    reduxStore.dispatch(
      alertAction.error("Your session has expired! Kindly Login again"),
    );
    return {};
  }
  return Promise.reject(error);
};

instance.interceptors.response.use(
  (response: AxiosResponse) => successHandler(response),
  (error: AxiosError) => errorHandler(error),
);

export default instance;
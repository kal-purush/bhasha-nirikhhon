import axios, { AxiosRequestConfig, AxiosInstance, AxiosResponse } from "axios";
import { config } from "process";

export interface ServiceResponse {
  data: any;
  msg: string;
  code: number;
}

axios.interceptors.request.use(
  (config: AxiosRequestConfig) => {
    return config;
  },
  (error) => {
    throw new Error(error);
  }
);

axios.interceptors.response.use(
  (response: AxiosResponse<ServiceResponse>) => {
    const { data } = response;
    // if (data.code !== 0) {
    //   throw new Error(data.msg);
    // }
    return data;
  },
  (error: any) => {
    const { status, data } = error.response;
    if (status !== 200) {
      throw new Error(data.message);
    }
  }
);

export default axios;
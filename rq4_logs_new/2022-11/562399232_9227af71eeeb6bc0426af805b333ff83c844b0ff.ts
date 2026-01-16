import axios, { AxiosInstance, AxiosRequestConfig } from "axios";
import type { APIInterface } from "./interface";

export const instance = axios.create({ baseURL: "/" });

class API implements APIInterface {
  private instance: AxiosInstance;

  constructor(instance: AxiosInstance) {
    this.instance = instance;
  }

  fetch = async (url: string, config?: AxiosRequestConfig<any>) => {
    const response = await this.instance.get(url, { ...config });
    return response;
  };
}

export default API;
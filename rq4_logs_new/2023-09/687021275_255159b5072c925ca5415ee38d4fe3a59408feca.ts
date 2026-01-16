import axios, { AxiosRequestConfig, AxiosResponse } from 'axios';

abstract class RestService {
  protected readonly baseURL: string;
  protected readonly config: AxiosRequestConfig;

  constructor(baseURL: string, config?: AxiosRequestConfig) {
    this.baseURL = baseURL;
    this.config = config || {};
  }

  protected async get<T>(
    url: string,
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {
    const mergedConfig = this.getMergedConfig(config);
    return axios.get<T>(`${this.baseURL}${url}`, mergedConfig);
  }

  protected async post<T>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {
    const mergedConfig = this.getMergedConfig(config);
    return axios.post<T>(`${this.baseURL}${url}`, data, mergedConfig);
  }

  protected async put<T>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {
    const mergedConfig = this.getMergedConfig(config);
    return axios.put<T>(`${this.baseURL}${url}`, data, mergedConfig);
  }

  protected async delete<T>(
    url: string,
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {
    const mergedConfig = this.getMergedConfig(config);
    return axios.delete<T>(`${this.baseURL}${url}`, mergedConfig);
  }

  private getMergedConfig(config?: AxiosRequestConfig): AxiosRequestConfig {
    const mergedConfig: AxiosRequestConfig = {
      ...this.config,
      ...config,
    };
    return mergedConfig;
  }
}

export default RestService;
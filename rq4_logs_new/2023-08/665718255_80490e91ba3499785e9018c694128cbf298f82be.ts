import axios from 'axios';
import AxiosInstance from '../Libs/axios/axios.config.ts';
import { API } from './api.ts';

export type SearchResponse<T> = {
  values: Array<T>;
};

export interface IHttpClient<T> {
  // TODO: Change the SearchBodyTyp to a more generic one
  post(body: SearchBody): Promise<Array<T>>;
}

export class HttpClient<M> implements IHttpClient<M> {
  private httClient: typeof AxiosInstance;

  constructor() {
    this.httClient = axios;
  }

  async post(body: SearchBody): Promise<M[]> {
    try {
      const { data } = await this.httClient.post<SearchResponse<M>>(
        API.SEARCH_API,
        body,
      );
      return data.values;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      throw new Error(error);
    }
  }
}
import axios, { AxiosError, AxiosInstance, AxiosResponse } from 'axios';
import axiosRetry from 'axios-retry';

interface FetchOptions {
  body?: any;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  headers?: Record<string, string>;
}

export interface ErrorResponse {
  error: boolean;
  message: string;
  response: AxiosResponse;
}

type FetchError = AxiosError<any, { data?: { message?: string } }>;

const defaultRetryConfig = {
  retries: 5,
  initialDelayMs: 100,
};

export default class HttpClient {
  client: AxiosInstance;
  constructor(baseURL: string, config = defaultRetryConfig) {
    this.client = axios.create({
      baseURL: baseURL,
      headers: { 'Content-Type': 'application/json' },
      withCredentials: true,
    });
    axiosRetry(this.client, {
      retries: config.retries,
      retryDelay: (retry) => {
        const delay = Math.pow(2, retry) * config.initialDelayMs;
        const jitter = delay * 0.1 * Math.random();
        return delay + jitter;
      },
      retryCondition: (err) =>
        axiosRetry.isNetworkOrIdempotentRequestError(err) ||
        err.response?.status === 429,
    });
  }

  async fetch(
    url: string,
    options: FetchOptions,
  ): Promise<any | ErrorResponse> {
    const { body, method, headers } = options;
    const req = {
      url,
      method,
      headers: {
        ...headers,
      },
      data: body,
    };

    try {
      const res = await this.client(req);
      return res.data;
    } catch (err) {
      const axiosError = err as FetchError;
      if (axiosError.response) {
        const data = axiosError.response.data;
        const message =
          data && data.message ? data.message : 'Something went wrong!';
        return {
          error: true,
          message,
          response: axiosError.response,
        };
      }
      throw new Error('connection error');
    }
  }
}
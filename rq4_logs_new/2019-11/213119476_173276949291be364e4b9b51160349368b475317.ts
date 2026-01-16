import axios from 'axios';
import { IHttpClient } from './iHttpClient';
import { IRequestConfig } from './iRequestConfig';

const axiosHttpClient: IHttpClient = async (requestConfig: IRequestConfig) => {
  const response = await axios({
    adapter: requestConfig.adapter,
    data: requestConfig.body,
    headers: requestConfig.headers,
    method: requestConfig.method,
    timeout: requestConfig.timeout,
    url: requestConfig.url
  });
  return response.data;
};

export default axiosHttpClient;
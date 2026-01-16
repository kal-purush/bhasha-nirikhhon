import axios from 'axios';
import { checkAuthToken } from '../helpers/user-helpers';

interface RequestConfig {
  url: string;
  method: 'get' | 'post' | 'put' | 'delete';
  data?: object;
  params?: object;
  headers?: {};
}

const request = async (config: RequestConfig): Promise<any> => {
  let authHeaders = {};
  const token = (await checkAuthToken()) ?? '';
  if (token) {
    authHeaders = {
      Authorization: `Bearer ${token}`,
      'x-environment': 'admin',
    };
  }
  try {
    return await axios({
      ...config,
      baseURL: process.env.REACT_APP_TWILIO_SEND_SMS_URL,
      headers: {
        ...config.headers,
        ...authHeaders,
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
    });
  } catch (error) {
    if (error) {
      localStorage.setItem('signout', '1');
    }
    throw error;
  }
};

export const smsservice = {
  get: async (url: string, params?: object, headers?: object) =>
    request({ url, method: 'get', params, headers }),
  post: async (url: string, data?: object) =>
    request({ url, method: 'post', data }),
  put: async (url: string, data: object) =>
    request({ url, method: 'put', data }),
  delete: async (url: string, data?: object) =>
    request({ url, method: 'delete', data }),
};
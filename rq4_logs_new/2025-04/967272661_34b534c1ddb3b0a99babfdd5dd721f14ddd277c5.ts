import axios from 'axios';
import { getCookieInClient, setCookieInClient } from './cookie/client';
import PATHS from '@/constants/paths';

const axiosClient = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL,
  timeout: 5000,
  headers: { 'Content-Type': 'application/json' },
  adapter: 'fetch',
});

axiosClient.interceptors.request.use((config) => {
  const token = getCookieInClient('accessToken');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

axiosClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;

    if (error.response && error.response.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        const refreshToken = getCookieInClient('refreshToken');
        const res = await axios.post(
          `${process.env.NEXT_PUBLIC_API_URL}/auth/refresh-token`,
          { refreshToken: refreshToken },
          {
            headers: {
              'Content-Type': 'application/json',
            },
            adapter: 'fetch',
          }
        );
        const newAccessToken = res.data?.accessToken;
        if (newAccessToken) {
          setCookieInClient('accessToken', newAccessToken);
          originalRequest.headers.Authorization = `Bearer ${newAccessToken}`;
          return axiosClient(originalRequest);
        }
      } catch (refreshError) {
        window.location.href = `${PATHS.LOGIN}`;
        return Promise.reject(refreshError);
      }
    }
    return Promise.reject(error);
  }
);

export default axiosClient;
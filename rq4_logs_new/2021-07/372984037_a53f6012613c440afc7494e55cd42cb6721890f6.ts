import axios from 'axios';
import { baseUrl } from './config';

const axiosInstance = axios.create();

axiosInstance.interceptors.request.use();

const joinUrl = (resources: string[]) => resources.join('/');

const getRequest = (url: string, params?: any) => axiosInstance.get(joinUrl([baseUrl, url]), { params });

const postRequest = (url: string, data?: any, params?: any) => axiosInstance.post(joinUrl([baseUrl, url]), data, { params });

const putRequest = (url: string, data?: any, params?: any) => axiosInstance.put(joinUrl([baseUrl, url]), data, { params });

const deleteRequest = (url: string, params?: any) => axiosInstance.delete(joinUrl([baseUrl, url]), { params });

export {
  getRequest,
  postRequest,
  putRequest,
  deleteRequest,
};
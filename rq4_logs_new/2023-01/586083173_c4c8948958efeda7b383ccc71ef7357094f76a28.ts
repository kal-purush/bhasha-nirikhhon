import axios, { AxiosError, AxiosInstance, AxiosRequestConfig } from 'axios';
import { axiosRequestConfiguration } from '@utils/Axios/Axios.config';

const initializeAxios = (config: AxiosRequestConfig): AxiosInstance => {
    const axiosInstance: AxiosInstance = axios.create(config);

    axiosInstance.interceptors.request.use(
        (request: AxiosRequestConfig) => request,
        (error: AxiosError) => Promise.reject(error)
    );

    return axiosInstance;
};

export const axiosInstance = initializeAxios(axiosRequestConfiguration);
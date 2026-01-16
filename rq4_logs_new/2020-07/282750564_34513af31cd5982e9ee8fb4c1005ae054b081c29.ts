import { AxiosRequestConfig } from 'axios';

export const baseURL = 'https://www.googleapis.com/books/v1/mylibrary';

export const axiosReq = (
	token: string,
	path = '',
	method: AxiosRequestConfig['method'] = 'GET'
): AxiosRequestConfig => {
	const headers: any = {
		Authorization: `Bearer ${token}`,
	};
	if (method === 'POST') {
		headers['Content-Type'] = 'application/json';
	}
	return {
		method,
		baseURL: baseURL + path,
		headers,
	};
};
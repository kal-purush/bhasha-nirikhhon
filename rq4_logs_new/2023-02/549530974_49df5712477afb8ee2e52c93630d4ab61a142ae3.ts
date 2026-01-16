import { DefaultHeaders, BASE_URL } from './Constants/HttpConfig';

export default {
  get(url: string, headers?: HeadersInit): Promise<Response> {
    return fetch(BASE_URL + url, {
      method: 'GET',
      headers: { ...DefaultHeaders, ...(headers ?? {}) },
    });
  },

  removeOrRetire(url: string, headers?: HeadersInit): Promise<Response> {
    return fetch(BASE_URL + url, {
      method: 'DELETE',
      headers: { ...DefaultHeaders, ...(headers ?? {}) },
    });
  },

  post<T>(url: string, body: T, headers?: HeadersInit): Promise<Response> {
    return fetch(BASE_URL + url, {
      method: 'POST',
      headers: { ...DefaultHeaders, ...(headers ?? {}) },
      body: JSON.stringify(body),
    });
  },

  put<T>(url: string, body: T, headers?: HeadersInit): Promise<Response> {
    return fetch(BASE_URL + url, {
      method: 'PUT',
      headers: { ...DefaultHeaders, ...(headers ?? {}) },
      body: JSON.stringify(body),
    });
  },
};
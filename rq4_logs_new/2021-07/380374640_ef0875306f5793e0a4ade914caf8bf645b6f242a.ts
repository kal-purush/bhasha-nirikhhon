import { BASE_URL, BASE_AUTH_URL } from '.';
import { MyChannelState } from '../lib/redux/reducers';

export const getUserApi = (accessToken: string, userId: string): Promise<Response> => {
  return fetch(BASE_URL + `/users/${userId}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer: ${accessToken}`
    },
  });
};

export const checkToken = (token: string, roles: string): Promise<Response> => {
  return fetch(BASE_AUTH_URL + '/checkAccess', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer: ${token}`
    },
    body: JSON.stringify({ roles }),
  });
};
/* Local dependencies */
import { Login } from '../../types/userTypes';
import { $api, setTokens } from './axois-api';

export async function register(data: any) {
  const response = await $api.post('register/', data);
  const newAccessToken = response?.data?.access;
  const newRefreshToken = response?.data?.refresh;

  setTokens(newAccessToken, newRefreshToken);
  $api.defaults.headers.Authorization = 'JWT ' + newAccessToken;

  return response;
}

export async function login(data: Login) {
  const response = await $api.post('token/obtain/', data);
  const newAccessToken = response?.data?.access;
  const newRefreshToken = response?.data?.refresh;

  setTokens(newAccessToken, newRefreshToken);
  $api.defaults.headers.Authorization = 'JWT ' + newAccessToken;

  return response;
}

const logout = () => {};

const authService = {
  register,
  login,
  logout,
};

export default authService;
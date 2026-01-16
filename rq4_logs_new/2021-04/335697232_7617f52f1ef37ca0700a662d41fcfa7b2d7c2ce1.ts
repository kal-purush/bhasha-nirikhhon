export const YANDEX_API_DOMAIN = 'https://ya-praktikum.tech';

export const YANDEX_API_URL = `${YANDEX_API_DOMAIN}/api/v2`;

export const YANDEX_API_PROXY_PREFIX = '/yaApiProxy';

export const getFullPath = (path: string) => {
  return `${YANDEX_API_URL}${path}`;
};

export const getFullPathFromProxy = (proxyPath: string) => {
  const path = proxyPath.replace(YANDEX_API_PROXY_PREFIX, '');
  return `${YANDEX_API_URL}${path}`;
};

export const getProxyPath = (path: string) => {
  return `${YANDEX_API_PROXY_PREFIX}${path}`;
};

export const API_PATH = {
  AUTH_USER: '/auth/user',
  AUTH_SIGNIN: '/auth/signin',
  AUTH_SIGNUP: '/auth/signup',
  AUTH_LOGOUT: '/auth/logout',

  USER_PROFILE: '/user/profile',
  USER_PROFILE_AVATAR: '/user/profile/avatar',
  USER_PASSWORD: '/user/password',
  USER_BY_ID: '/user',

  LEADERBOARD_ALL: '/leaderboard/all',
  LEADERBOARD_UPDATE: '/leaderboard',
} as const;
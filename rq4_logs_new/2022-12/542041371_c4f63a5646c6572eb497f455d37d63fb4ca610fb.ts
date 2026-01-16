export const API_YANDEX_DOMAIN = 'https://ya-praktikum.tech';
export const API_YANDEX_PATH = '/api/v2';
export const API_YANDEX_URL = `${API_YANDEX_DOMAIN}${API_YANDEX_PATH}`;
export const IS_DEV = process.env.NODE_ENV === 'development';
export const SERVER_PORT = Number(process.env.SERVER_PORT) || 3001;
export const CLIENT_PORT = Number(process.env.CLIENT_PORT) || 3000;
export const PROXY_PATH_REWRITE = `^/api/user`;
export const APP_DEV_URL = `http://localhost:${CLIENT_PORT}`;
export const APP_PROD_URL = 'http://stack-overflow-runners.ya-praktikum.tech';
export const APP_CURRENT_URL = IS_DEV ? APP_DEV_URL : APP_PROD_URL;
export const APP_CURRENT_DOMAIN = IS_DEV
  ? `http://localhost:${CLIENT_PORT}`
  : 'http://stack-overflow-runners.ya-praktikum.tech';
export const PROXY_PATH_REWRITE_OPTIONS = {
  '^/api/user/$': 'api/v2/auth/user',
  '^/api/user/profile': 'api/v2/user/profile',
  '^/api/resources': 'api/v2/resources',
  '^/api/user/signin': 'api/v2/auth/signin',
  '^/api/user/signup': 'api/v2/auth/signup',
  '^/api/user/logout$': 'api/v2/auth/logout',
  '^/api/user/oauth/yandex': 'api/v2/oauth/yandex',
  '^/api/user/oauth/yandex/service-id': 'api/v2/oauth/yandex/service-id',
  '^/api/leaderboard': 'api/v2/leaderboard',
};

const consts = {
  API_YANDEX_DOMAIN,
  API_YANDEX_PATH,
  IS_DEV,
  SERVER_PORT,
  CURRENT_DOMAIN: APP_CURRENT_DOMAIN,
  PROXY_PATH_REWRITE,
  CLIENT_PORT,
  APP_DEV_URL,
  APP_PROD_URL,
  APP_CURRENT_URL,
  PROXY_PATH_REWRITE_OPTIONS,
};

export default consts;
import client from 'eqb-request-client';

function getApiUri(): string {
  const apiUriPreferences = {
    production: process.env.REACT_APP_PROD_API_URI,
    development: process.env.REACT_APP_DEV_API_URI,
    test: process.env.REACT_APP_TEST_API_URI,
  };
  const addr = apiUriPreferences[process.env.NODE_ENV || 'development'];
  if (!addr) {
    throw new Error(`API URI NOT DEFINED - ${process.env.NODE_ENV}`);
  }
  return addr;
}

const OPTIONS: Parameters<typeof client>[1] = {
  onTokenResignFail: () => {
    localStorage.removeItem('refresh-token');
    sessionStorage.removeItem('access-token');
  },
  loggerOnError: console.error,
  loggerOnInfo: console.info,
  accessTokenHeaderKey: 'X-Access-Token',
  accessTokenResolver: {
    get: () => sessionStorage.getItem('access-token') || 'null',
    set: (value: string) => sessionStorage.setItem('access-token', value),
  },
  refreshTokenResolver: {
    get: () => localStorage.getItem('refresh-token') || 'null',
    set: (value: string) => localStorage.setItem('refresh-token', value),
  },
  tokenResignEndpiont: 'auth/resign',
};

export default client(getApiUri(), OPTIONS);
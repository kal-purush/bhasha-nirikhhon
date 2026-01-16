import { isStr } from '@utils';

interface AppConifg {
  env: AppEnv;
  name: string;
  version: string;
  title?: string;
  company?: string;
  description?: string;
  url?: string;
  sentry: {
    dsn: string;
  };
}

type AppEnv = 'development' | 'production';

const isAppEnv = (val: unknown): val is AppEnv => isStr(val) && ['development', 'production'].includes(val);

export const config: AppConifg = {
  env: isAppEnv(APP_ENV) ? APP_ENV : 'production',
  name: isStr(APP_NAME) ? APP_NAME : '@kremen/transport-web',
  version: isStr(APP_VERSION) ? APP_VERSION : '0.0.0',
  title: APP_TITLE,
  company: APP_COMPANY,
  description: APP_DESCRIPTION,
  url: APP_URL,
  sentry: {
    dsn: isStr(SENTRY_DSN) ? SENTRY_DSN : '',
  },
};
import * as path from 'path';

export const USER_HOME_PATH = process.env[process.platform == 'win32' ? 'USERPROFILE' : 'HOME'] ?? '';

export const CLASP_LOGIN_FILE_NAME = '.clasprc.json';
export const DEFAULT_CLAPS_LOGIN_FILE_PATH = path.join(USER_HOME_PATH, CLASP_LOGIN_FILE_NAME);

export const CLAPS_SETTING_FILE_NAME = '.clasp.json';
export const DEFAULT_CLAPS_SETTING_FILE_PATH = path.join(process.cwd(), CLAPS_SETTING_FILE_NAME);

export const ENVIRONMENT_FILE_NAME = '.env';
export const DEFAULT_ENVIRONMENT_FILE_PATH = path.join(process.cwd(), ENVIRONMENT_FILE_NAME);

export const SCRIPT_OPTION = {
  cd: 'CD',
  deploymentName: 'deploymentName',
  clasprcPath: 'clasprcPath',
  claspPath: 'claspPath',
} as const;

export const INITIAL_CLASP_LOGIN_INFO = {
  token: {
    access_token: '',
    refresh_token: '',
    scope:
      'https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/service.management https://www.googleapis.com/auth/script.deployments https://www.googleapis.com/auth/logging.read https://www.googleapis.com/auth/script.webapp.deploy https://www.googleapis.com/auth/userinfo.profile openid https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/script.projects https://www.googleapis.com/auth/drive.metadata.readonly',
    token_type: 'Bearer',
    id_token: '',
    expiry_date: 0,
  },
  oauth2ClientSettings: {
    clientId: '',
    clientSecret: '',
    redirectUri: 'http://localhost',
  },
  isLocalCreds: false,
};
export const INITIAL_CLASP_SETTING = {
  scriptId: '',
  rootDir: 'dist/',
  fileExtension: 'js',
};

export type ClaspLoginInfo = typeof INITIAL_CLASP_LOGIN_INFO;
export type ClaspSetting = typeof INITIAL_CLASP_SETTING;
import path from 'path';
import { EggAppConfig, PowerPartial, EggAppInfo } from 'beidou';
import privateConfig from './private.config';
// 应用本身的配置 Scheme
export interface IStaticConfig {
  static: {
    dir: Array<{ prefix: string; dir: string }>;
  };
}

export default (appInfo: EggAppInfo) => {
  const config: PowerPartial<EggAppConfig & IStaticConfig> = {};

  config.view = {
    defaultExtension: '.tsx',
  };

  config.router = {
    exts: [ '.jsx', '.ts', '.tsx' ],
    entry: 'index',
  };

  config.isomorphic = {
    // babel: false,
  };

  config.webpack = {
    // your webpack config file
    custom: {
      // configPath: path.resolve(__dirname, './webpack.config.ts'),
    },
    resolve: {
      extensions: [ '.json', '.js', '.jsx', '.ts', '.tsx' ],
      alias: {
        client: path.join(__dirname, '../client'),
      },
    },
  };

  config.static = {
    dir: [
      {
        prefix: '/public',
        dir: path.join(appInfo.baseDir, '/app/public'),
      },
      {
        prefix: '/build',
        dir: path.join(appInfo.baseDir, '/build'),
      },
    ],
  } as any; // todo: will remove after egg update
  return { ...config, ...privateConfig(appInfo) };
};
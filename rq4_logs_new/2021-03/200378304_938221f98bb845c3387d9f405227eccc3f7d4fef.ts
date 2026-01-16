import merge from 'lodash.merge';
import type { TsConfigJson } from 'type-fest';

import { Config } from './common';
import defaultConfig from './default';

const base: TsConfigJson = merge<
  Record<string, unknown>,
  TsConfigJson,
  TsConfigJson
>({}, defaultConfig.base, {
  extends: '@tsconfig/node10',
});

const production: TsConfigJson = merge<
  Record<string, unknown>,
  TsConfigJson,
  TsConfigJson
>({}, defaultConfig.production, base);

const config: Config = {
  description: 'CLI TS Config',
  suffix: 'cli',
  base,
  production,
};

export default config;
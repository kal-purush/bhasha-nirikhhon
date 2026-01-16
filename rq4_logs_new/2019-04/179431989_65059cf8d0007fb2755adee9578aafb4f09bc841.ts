export interface IAppFlags {
  featureEnabled: boolean;
}

const defaultFlags: IAppFlags = {
  featureEnabled: false,
};

import { Config } from '@shared';

const api = Config.configureFlags(defaultFlags);

export const Flags = {
  load: api.load,
  latest: api.latest,
  withLatest: api.withLatest,
};
import mock from 'mock-require';

import {
  Subject
} from 'rxjs';

import {
  take
} from 'rxjs/operators';

import {
  clearProtractorEnvironmentConfig,
  getProtractorEnvironmentConfig
} from '../../../../shared/protractor-environment-utils';

import {
  SkyuxProtractorPluginConfig
} from './protractor-plugin-config';

import {
  SkyuxProtractorPlugin
} from './protractor.plugin';

describe('protractor webpack plugin', () => {

  let mockCompiler: any;
  let hookDone: Subject<void>;

  beforeEach(() => {
    hookDone = new Subject<void>();

    mockCompiler = {
      hooks: {
        done: {
          async tapPromise(_pluginName: string, callback: () => void) {
            await callback();
            hookDone.next();
          }
        }
      }
    };
  });

  afterEach(() => {
    mock.stopAll();
    clearProtractorEnvironmentConfig();
    hookDone.complete();
  });

  function getPlugin(options: Partial<SkyuxProtractorPluginConfig> = {}): SkyuxProtractorPlugin {
    const SkyuxProtractorPlugin = mock.reRequire('./protractor.plugin').SkyuxProtractorPlugin;

    const plugin = new SkyuxProtractorPlugin({
      ...{
        hostUrlFactory() {
          return Promise.resolve('https://foo.blackbaud.com/')
        }
      },
      ...options
    });

    return plugin;
  }

  it('should set host URL for protractor config', async (done) => {
    const plugin = getPlugin();

    plugin.apply(mockCompiler);

    hookDone.pipe(take(1)).subscribe(() => {
      expect(getProtractorEnvironmentConfig()?.skyuxHostUrl).toEqual('https://foo.blackbaud.com/');
      done();
    });
  });

});
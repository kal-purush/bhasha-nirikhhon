import mock from 'mock-require';

import {
  SkyuxConfig
} from '../shared/skyux-config';

describe('skyux config utils', () => {
  let fileExists: boolean;
  let mockSkyuxConfig: Partial<SkyuxConfig>;

  beforeEach(() => {
    fileExists = true;
    mockSkyuxConfig = {};

    mock('fs-extra', {
      existsSync() {
        return fileExists;
      },
      readJsonSync() {
        return mockSkyuxConfig;
      }
    });
  });

  afterEach(() => {
    mock.stopAll();
  });

  it('should return defaults', () => {
    const util = mock.reRequire('./skyux-config-utils');
    expect(util.getSkyuxConfig()).toEqual({
      host: {
        url: 'https://host.nxt.blackbaud.com/'
      }
    });
  });

  it('should add trailing slash to host url', () => {
    mockSkyuxConfig = {
      host: {
        url: 'https://foo.blackbaud.com'
      }
    };
    const util = mock.reRequire('./skyux-config-utils');
    expect(util.getSkyuxConfig()).toEqual({
      host: {
        url: 'https://foo.blackbaud.com/'
      }
    });
  });

  it('should throw an error if skyuxconfig.json does not exist', () => {
    fileExists = false;
    const util = mock.reRequire('./skyux-config-utils');
    expect(util.getSkyuxConfig).toThrowError(
      'A skyuxconfig.json file was not found at the project root.'
    );
  });
});
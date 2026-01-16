import mock from 'mock-require';

describe('skyux config loader', () => {

  beforeEach(() => {
    mock('../../../../shared/skyux-config-utils', {
      getSkyuxConfig() {
        return {
          host: {
            url: 'https://foo.blackbaud.com'
          }
        };
      }
    });
  });

  afterEach(() => {
    mock.stopAll();
  });

  it('should replace processed-skyuxconfig.json contents', () => {
    const loader = mock.reRequire('./skyux-config.loader').default;

    const result = loader.apply();

    expect(result).toEqual('{"host":{"url":"https://foo.blackbaud.com"}}');
  });

});
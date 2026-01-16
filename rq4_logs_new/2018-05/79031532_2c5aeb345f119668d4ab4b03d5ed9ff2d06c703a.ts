import nc from './nc';

describe('nc(process.env).defaults(defaults)', () => {
  const env = {
    PORT: '8080',
    AUTH_JWT_SECRET: 'Test Secret',
    FACEBOOK_SECRET: '7fd670edc01845e9aca2f3d70d11339d',
  };
  const defaults = {
    port: 3000,
    databaseUrl: 'sqlite:database.sqlite',
    api: {
      serverUrl: 'http://localhost:${port}',
      timeout: 120000,
    },
    auth: {
      jwt: { secret: 'Default Secret' },
      facebook: {
        id: '${FACEBOOK_ID:965881723876770}',
        secret: '${FACEBOOK_SECRET:c0e8f5ba8f8e41688eb385a24a8a40b0}',
      },
    },
  };

  it('should overwrite defaults.port by process.env.PORT', () => {
    const config = nc(env).defaults(defaults);
    expect(config.port).toBe(env.PORT);
  });

  it('should not overwrite defaults.databaseUrl', () => {
    const config = nc(env).defaults(defaults);
    expect(config.databaseUrl).toBe(defaults.databaseUrl);
  });

  it('should resolve placeholder with a value', () => {
    const config = nc(env).defaults(defaults);
    expect(config.api.serverUrl).toBe(`http://localhost:${env.PORT}`);
  });

  it('should resolve placeholder with a environment variables', () => {
    const config = nc(env).defaults(defaults);
    expect(config.auth.facebook.secret).toBe(env.FACEBOOK_SECRET);
  });

  it('should resolve placeholder with a default value', () => {
    const config = nc(env).defaults(defaults);
    expect(config.auth.facebook.id).toBe('965881723876770');
  });

  it('should overwrite default.auth.jwt.secret by process.env.AUTH_JWT_SECRET', () => {
    const config = nc(env).defaults(defaults);
    expect(config.auth.jwt.secret).toBe(env.AUTH_JWT_SECRET);
  });

  it('should keep data type', () => {
    const config = nc(env).defaults(defaults);
    expect(config.api.timeout).toBe(defaults.api.timeout);
  });
});
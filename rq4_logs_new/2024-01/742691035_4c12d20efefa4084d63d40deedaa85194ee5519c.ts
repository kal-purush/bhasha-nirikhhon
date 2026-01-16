import { redirect } from '@tanstack/react-router';

import { logger } from './constants';
import { redirectBeforeLoadRequireAuthentication } from './redirect';

jest.mock('@tanstack/react-router', () => ({
  redirect: jest.fn()
}));

jest.mock('./constants', () => ({
  logger: {
    log: jest.fn()
  }
}));

describe('redirectBeforeLoadRequireAuthentication', () => {
  it('log and redirect to /signin if not authenticated and path is not /signin or /signup', () => {
    const middleware = redirectBeforeLoadRequireAuthentication();
    const context = { auth: false };
    const location = { href: '/private' };

    expect(() => middleware({ context, location })).toThrow();
    expect(logger.log).toHaveBeenCalledWith('redirect:from', location.href);
    expect(redirect).toHaveBeenCalledWith({
      to: '/signin',
      search: { redirect: location.href }
    });
  });

  it('not redirect if navigating to /signin or /signup without authentication', () => {
    const middleware = redirectBeforeLoadRequireAuthentication();
    const context = { auth: false };
    const location = { href: '/signin' };

    expect(() => middleware({ context, location })).not.toThrow();
    expect(logger.log).toHaveBeenCalledWith('redirect:normal-user-flow');
  });

  it('log and redirect to a specified path if authenticated', () => {
    const toPath = '/dashboard';
    const middleware = redirectBeforeLoadRequireAuthentication(toPath);
    const context = { auth: true };
    const location = { href: '/private' };

    expect(() => middleware({ context, location })).toThrow();
    expect(logger.log).toHaveBeenCalledWith('redirect:to', toPath);
    expect(redirect).toHaveBeenCalledWith({ to: toPath });
  });

  it('log normal user flow if the user is authenticated and no specific redirection path is provided', () => {
    const middleware = redirectBeforeLoadRequireAuthentication();
    const context = { auth: true };
    const location = { href: '/another-page' };

    expect(() => middleware({ context, location })).not.toThrow();
    expect(logger.log).toHaveBeenCalledWith('redirect:normal-user-flow');
  });
});
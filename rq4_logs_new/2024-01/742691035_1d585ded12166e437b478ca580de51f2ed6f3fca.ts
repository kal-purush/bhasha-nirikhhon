import { Authentication } from './authentication';
import { logger } from './constants';
import { AuthMethods } from './types';

jest.mock('./constants', () => ({
  logger: {
    log: jest.fn()
  }
}));

const mockEngine: AuthMethods = {
  signIn: jest.fn(),
  signUp: jest.fn(),
  signOut: jest.fn()
};

describe('Authentication - signIn', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('log success when signIn is successful', async () => {
    (mockEngine.signIn as jest.Mock).mockResolvedValueOnce(true);

    const auth = new Authentication(mockEngine);
    await auth.signIn();

    expect(mockEngine.signIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-in:success');
  });

  it('log error when signIn fails', async () => {
    const error = new Error('Failed to sign in');
    (mockEngine.signIn as jest.Mock).mockRejectedValueOnce(error);

    const auth = new Authentication(mockEngine);
    await auth.signIn();

    expect(mockEngine.signIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-in:error', error);
  });
});

describe('Authentication - signUp', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('log success when signUp is successful', async () => {
    (mockEngine.signIn as jest.Mock).mockResolvedValueOnce(true);

    const auth = new Authentication(mockEngine);
    await auth.signUp();

    expect(mockEngine.signIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-up:success');
  });

  it('log error when signUp fails', async () => {
    const error = new Error('Failed to sign up');
    (mockEngine.signIn as jest.Mock).mockRejectedValueOnce(error);

    const auth = new Authentication(mockEngine);
    await auth.signUp();

    expect(mockEngine.signIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-up:error', error);
  });
});

describe('Authentication - signOut', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('log success when signOut is successful', async () => {
    (mockEngine.signOut as jest.Mock).mockResolvedValueOnce(true);

    const auth = new Authentication(mockEngine);
    await auth.signOut();

    expect(mockEngine.signOut).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-out:success');
  });

  it('log error when signOut fails', async () => {
    const error = new Error('Failed to sign out');
    (mockEngine.signOut as jest.Mock).mockRejectedValueOnce(error);

    const auth = new Authentication(mockEngine);
    await auth.signOut();

    expect(mockEngine.signOut).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-out:error', error);
  });
});
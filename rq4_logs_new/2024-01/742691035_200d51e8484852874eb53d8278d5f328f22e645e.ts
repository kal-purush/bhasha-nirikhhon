import { useNavigate } from '@tanstack/react-router';
import { act, renderHook } from '@testing-library/react-hooks';

import { Authentication } from '@modules/authentication/authentication';
import { getSearchParams } from '@utilities/get-search-params';

import { logger } from './constants';
import { useAuthCallbackHandler } from './useAuthCallbackHandler';

jest.mock('@tanstack/react-router', () => ({
  useNavigate: jest.fn()
}));

jest.mock('@utilities/get-search-params', () => ({
  getSearchParams: jest.fn(() => ({ get: jest.fn() }))
}));

jest.mock('./constants', () => ({
  logger: {
    log: jest.fn()
  }
}));

describe('useAuthCallbackHandler', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('signIn navigates to redirect path on success with redirect', async () => {
    const mockSignIn = jest.fn();
    const AuthenticationMock = { signIn: mockSignIn };
    const mockNavigate = jest.fn();
    (useNavigate as jest.Mock).mockReturnValue(mockNavigate);
    (getSearchParams as jest.Mock).mockReturnValue({
      get: () => '/custom/redirect'
    });

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signIn();
    });

    expect(mockSignIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-in:success');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/custom/redirect' });
  });

  it('signIn navigates to redirect path on success without redirect', async () => {
    const mockSignIn = jest.fn();
    const AuthenticationMock = { signIn: mockSignIn };
    const mockNavigate = jest.fn();
    (useNavigate as jest.Mock).mockReturnValue(mockNavigate);
    (getSearchParams as jest.Mock).mockReturnValue({
      get: () => null
    });

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signIn();
    });

    expect(mockSignIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-in:success');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/me/home' });
  });

  it('signIn logs error on failure', async () => {
    const mockSignIn = jest.fn().mockRejectedValue(new Error('Error'));
    const AuthenticationMock = { signIn: mockSignIn };

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signIn();
    });

    expect(mockSignIn).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-in:error', expect.any(Error));
  });

  it('signUp navigates to redirect path on success with redirect', async () => {
    const mockSignUp = jest.fn();
    const AuthenticationMock = { signUp: mockSignUp };
    const mockNavigate = jest.fn();
    (useNavigate as jest.Mock).mockReturnValue(mockNavigate);
    (getSearchParams as jest.Mock).mockReturnValue({
      get: () => '/custom/redirect'
    });

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signUp();
    });

    expect(mockSignUp).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-up:success');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/custom/redirect' });
  });

  it('signUp navigates to redirect path on success without redirect', async () => {
    const mockSignUp = jest.fn();
    const AuthenticationMock = { signUp: mockSignUp };
    const mockNavigate = jest.fn();
    (useNavigate as jest.Mock).mockReturnValue(mockNavigate);
    (getSearchParams as jest.Mock).mockReturnValue({
      get: () => null
    });

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signUp();
    });

    expect(mockSignUp).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-up:success');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/me/home' });
  });

  it('signUp logs error on failure', async () => {
    const mockSignUp = jest.fn().mockRejectedValue(new Error('Error'));
    const AuthenticationMock = { signUp: mockSignUp };

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signUp();
    });

    expect(mockSignUp).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-up:error', expect.any(Error));
  });

  it('signOut navigates to home on success', async () => {
    const mockSignOut = jest.fn();
    const AuthenticationMock = { signOut: mockSignOut };

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signOut();
    });

    expect(mockSignOut).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith('sign-out:success');
    expect((useNavigate() as jest.Mock).mock.calls[0][0]).toEqual({ to: '/' });
  });

  it('signOut logs error on failure', async () => {
    const mockSignOut = jest.fn().mockRejectedValue(new Error('Error'));
    const AuthenticationMock = { signOut: mockSignOut };

    const { result } = renderHook(() =>
      useAuthCallbackHandler(AuthenticationMock as unknown as Authentication)
    );

    await act(async () => {
      await result.current.signOut();
    });

    expect(mockSignOut).toHaveBeenCalled();
    expect(logger.log).toHaveBeenCalledWith(
      'sign-out:error',
      expect.any(Error)
    );
  });
});
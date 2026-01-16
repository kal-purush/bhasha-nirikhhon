import { useNavigate } from '@tanstack/react-router';

import { Authentication } from '@modules/authentication/authentication';
import { getSearchParams } from '@utilities/get-search-params';

import { logger } from './constants';

export const useAuthCallbackHandler = (engine: Authentication | undefined) => {
  const navigate = useNavigate();
  const { get } = getSearchParams();
  const redirect = get('redirect');

  const signIn = async () => {
    try {
      await engine?.signIn();
      logger.log('sign-in:success');
      const to = redirect ?? '/me/home';
      navigate({ to });
    } catch (error) {
      logger.log('sign-in:error', error);
    }
  };
  const signUp = async () => {
    try {
      await engine?.signUp();
      logger.log('sign-up:success');
      const to = redirect ?? '/me/home';
      navigate({ to });
    } catch (error) {
      logger.log('sign-up:error', error);
    }
  };
  const signOut = async () => {
    try {
      await engine?.signOut();
      logger.log('sign-out:success');
      navigate({ to: '/' });
    } catch (error) {
      logger.log('sign-out:error', error);
    }
  };

  return { signIn, signUp, signOut };
};
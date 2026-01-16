import { logger } from './constants';
import { AuthMethods } from './types';

export class Authentication implements AuthMethods {
  private engine: AuthMethods;
  constructor(engine: AuthMethods) {
    this.engine = engine;
  }
  signIn = async () => {
    try {
      await this.engine.signIn();
      logger.log('sign-in:success');
    } catch (error) {
      logger.log('sign-in:error', error);
    }
  };

  signUp = async () => {
    try {
      await this.signIn();
      logger.log('sign-up:success');
    } catch (error) {
      logger.log('sign-up:error', error);
    }
  };

  signOut = async () => {
    try {
      await this.engine.signOut();
      logger.log('sign-out:success');
    } catch (error) {
      logger.log('sign-out:error', error);
    }
  };
}
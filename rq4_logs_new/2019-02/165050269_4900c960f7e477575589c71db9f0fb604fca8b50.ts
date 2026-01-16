import { AuthServiceState, JwtAuthService } from '@app/core/services';
import {
  localizationServiceSpyFactory,
  localStorageServiceSpyFactory,
  routerSpyFactory
} from '../spies';
import { authInfoFakeFactory } from '@app/core/test-doubles';

const authInfo = authInfoFakeFactory();

export class JwtAuthServiceStub extends JwtAuthService {
  constructor() {
    super(
      null,
      localStorageServiceSpyFactory(),
      routerSpyFactory(),
      localizationServiceSpyFactory()
    );
  }

  async signIn(userName: string, password: string): Promise<void> {
    if (userName === authInfo.userName && password === authInfo.password) {
      await this.signInCompleted(authInfo.token);
    } else {
      this.signInFailed();
    }
  }
}
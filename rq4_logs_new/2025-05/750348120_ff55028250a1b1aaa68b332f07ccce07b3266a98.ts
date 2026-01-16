import { container } from '@utils';
import { IndexPage, ResetPasswordPage } from '@pages/backoffice';
import {
  UserMfaAuthDynamicFixtures,
  UserMfaAuthStaticFixtures,
} from '../../../support/types/backoffice/multi-factor-authentication';
import { UserLoginScenario, UserLogoutScenario, UserMfaLoginScenario } from '@scenarios/backoffice';
import { UserMfaActivationScenario } from '../../../support/scenarios/backoffice/user-mfa-activation-scenario';
import { retryableBefore } from '../../../support/e2e';

(['suite'].includes(Cypress.env('repositoryId')) ? describe : describe.skip)(
  'user mfa auth [suite]',
  { tags: ['@backoffice', '@user-account-management'] },
  (): void => {
    const backofficeIndexPage = container.get(IndexPage);
    const userResetPasswordPage = container.get(ResetPasswordPage);
    const mfaActivationScenario = container.get(UserMfaActivationScenario);
    const mfaLoginScenario = container.get(UserMfaLoginScenario);
    const userLoginScenario = container.get(UserLoginScenario);
    const userLogoutScenario = container.get(UserLogoutScenario);

    let dynamicFixtures: UserMfaAuthDynamicFixtures;
    let staticFixtures: UserMfaAuthStaticFixtures;

    retryableBefore((): void => {
      ({ staticFixtures, dynamicFixtures } = Cypress.env());
    });

    it('should verify successful MFA activation and subsequent authenticated login', (): void => {
      userLoginScenario.execute({
        username: dynamicFixtures.rootUserOne.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();
      mfaActivationScenario.execute(dynamicFixtures.rootUserOne.username);

      userLogoutScenario.execute();
      mfaLoginScenario.execute({
        username: dynamicFixtures.rootUserOne.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();
    });

    it('should ensure proper error handling when invalid MFA verification code is provided', (): void => {
      userLoginScenario.execute({
        username: dynamicFixtures.rootUserTwo.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();
      mfaActivationScenario.execute(dynamicFixtures.rootUserTwo.username);

      userLogoutScenario.execute();
      mfaLoginScenario.executeWithInvalidCode(
        {
          username: dynamicFixtures.rootUserTwo.username,
          password: staticFixtures.defaultPassword,
        },
        staticFixtures
      );
    });

    it("should change password and login using a new one without MFA if it's not enabled", (): void => {
      userLoginScenario.execute({
        username: dynamicFixtures.rootUserThree.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();

      userResetPasswordPage.visit();
      userResetPasswordPage.changePassword(staticFixtures.defaultPassword, staticFixtures.newPassword);

      userLogoutScenario.execute();

      userLoginScenario.execute({
        username: dynamicFixtures.rootUserThree.username,
        password: staticFixtures.newPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();
    });

    it('should verify successful MFA deactivation and subsequent login without MFA', (): void => {
      userLoginScenario.execute({
        username: dynamicFixtures.rootUserFour.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();
      mfaActivationScenario.execute(dynamicFixtures.rootUserFour.username);

      userLogoutScenario.execute();
      mfaLoginScenario.execute({
        username: dynamicFixtures.rootUserFour.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();

      mfaActivationScenario.deactivate(dynamicFixtures.rootUserFour.username);

      userLogoutScenario.execute();
      userLoginScenario.execute({
        username: dynamicFixtures.rootUserFour.username,
        password: staticFixtures.defaultPassword,
      });

      backofficeIndexPage.visit();
      backofficeIndexPage.assertPageLocation();
    });
  }
);
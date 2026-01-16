import { Page, test } from '@playwright/test';

import { SignupHelper } from '@/helpers/SignupHelper/signup.helper';
import { SignInHelper } from '@/helpers/SigninHelper/signIn.helper';

export const PROCESS_SIGNUP = test.extend<{ signup: void }>({
    signup: [
        async ({ page }: { page: Page }, use: () => any) => {
            const signupPage = new SignupHelper(page);
            await signupPage.init();
            const username = SignupHelper.genRandomEmail();
            const name = 'test';
            const password = '123456';
            const confirm_password = '123456';

            await signupPage.fillSignup({
                name: name,
                email: username,
                password: password,
                confirm_password: confirm_password,
            });

            await signupPage.clickButton('Next →');

            await signupPage.fillOtp('111111', 6);

            await signupPage.clickButton('Verify →');
            await signupPage.clickButton('Continue →');
            await signupPage.fillInput(name, { name: 'organization_name' });

            await signupPage.clickButton('Continue');
            const signin = new SignInHelper(page);
            await signin.init();
            await signin.checkDashboard({
                username: username,
                password: password,
            });
            await page.context().storageState({ path: 'state.json' });

            await use();
        },

        {
            auto: true,
        },
    ],
});
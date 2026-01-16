import { ResetPasswordHelper } from '@/helpers/SigninHelper/resetpassword.helper';
import { SignInHelper } from '@/helpers/SigninHelper/signIn.helper';
import { test, expect } from '@playwright/test';

test.describe('Reset Password', () => {
    test('Reset Password page is open', async ({ page }) => {
        const signIn = new SignInHelper(page);
        await signIn.init();
        const username = 'newtestauto@company.com';
        const password = '123456';
        await signIn.checkDashboard({
            username: username,
            password: password,
        });
        const resetPassword = new ResetPasswordHelper(page);
        await resetPassword.init();
        await resetPassword.resetPasswordPage();

        await expect(page.getByText('Change Password')).toHaveCount(1);
    });
});
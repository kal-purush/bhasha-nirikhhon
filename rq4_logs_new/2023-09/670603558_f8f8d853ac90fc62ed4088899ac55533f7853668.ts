import { ForgotPasswordHelper } from '@/helpers/SigninHelper/forgotpassword.helper';
import { expect, test } from '@playwright/test';
test.describe('Forgot Password', () => {
    test('without details', async ({ page }) => {
        const forgotpassword = new ForgotPasswordHelper(page);
        await forgotpassword.init();
        await forgotpassword.clickButton('Next →');

        await expect(
            page.locator('//span[contains(@class, "label-text")]')
        ).toHaveCount(1);
    });
    test('without Email Field', async ({ page }) => {
        const forgotpassword = new ForgotPasswordHelper(page);
        await forgotpassword.init();
        await forgotpassword.forgotPasswordPage('');
        await expect(forgotpassword.errorMessage()).toBe(
            'Invalid Email Address'
        );
    });
    test('with invalid Email Field', async ({ page }) => {
        const forgotpassword = new ForgotPasswordHelper(page);
        await forgotpassword.init();
        await forgotpassword.forgotPasswordPage('test');
        expect(forgotpassword.errorMessage()).toBe('Invalid Email Address');
    });
    test('with valid Email Field', async ({ page }) => {
        const forgotpassword = new ForgotPasswordHelper(page);
        await forgotpassword.init();
        await forgotpassword.forgotPasswordPage('test@gmail.com');
    });
});
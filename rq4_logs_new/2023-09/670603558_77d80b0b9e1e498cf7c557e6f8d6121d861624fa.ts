import { CustofeildHelper } from '@/helpers/CustomefeildHelper/customefeild.helper';
import { test, expect } from '@playwright/test';
import { SignInHelper } from '@/helpers/SigninHelper/signIn.helper';

test.describe('CustomeFeild', () => {
    test('Check the page opening', async ({ page }) => {
        const signin = new SignInHelper(page);
        await signin.init();
        const username = 'newtestauto@company.com';
        const password = '123456';
        await signin.checkDashboard({
            username: username,
            password: password,
        });

        const customefeild = new CustofeildHelper(page);
        await customefeild.init();
        await expect(page.getByText('Custome Feilds')).toHaveCount(1);
    });
});
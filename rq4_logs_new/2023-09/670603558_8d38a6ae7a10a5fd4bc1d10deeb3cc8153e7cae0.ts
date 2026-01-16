import { GradesHelper } from '@/helpers/GradesHelper/grade.helper';
import { test, expect } from '@playwright/test';
import { SignInHelper } from '@/helpers/SigninHelper/signIn.helper';

test.describe('Grades', () => {
    test('without details', async ({ page }) => {
        const signin = new SignInHelper(page);

        await signin.init();
        const username = 'newtestauto@company.com';
        const password = '123456';
        await signin.checkDashboard({
            username: username,
            password: password,
        });

        const grades = new GradesHelper(page);
        await grades.init();

        await expect(page.getByText('Grades')).toHaveCount(4);
    });
});
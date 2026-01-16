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
        await expect(page.getByText('Custom')).toHaveCount(2);
    });
    test('Check Vendor and Click Vendor Tab', async ({ page }) => {
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

        await customefeild.clickExpenseTab('Vendor');
        await expect(
            page.getByRole('tab', { name: 'Vendor', exact: true })
        ).toHaveCount(1);
    });

    test('Check Vendor Tab and Click Add New', async ({ page }) => {
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

        await customefeild.clickExpenseTab('Vendor');
        await customefeild.clickButton('Add New');
        await expect(page.getByText('Add Vendor Custom Field')).toHaveCount(1);
    });
    test('Add Vendor With Empty Feilds', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');
        await customefeild.clickButton('Add New');

        await customefeild.clickButton('Save');

        await expect(page.getByRole('button', { name: 'Save' })).toHaveCount(1);
    });

    test('Add Vendor Without Name Feilds', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');

        await customefeild.AddExpenseCustomeFeild('', 'Text', 1);

        expect(await customefeild.errorMessage()).toBe(
            'Field Name is required'
        );
    });
    test('Add Vendor Without Type Feilds', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');
        await customefeild.AddExpenseCustomeFeild('Test1', '', 1);

        expect(await customefeild.errorMessage()).toBe(
            'Field Type is required'
        );
    });
    test('Add Expense With Text type', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');
        await customefeild.AddExpenseWithTextType('Test1', 'Text', 1, 'Test1');
        await customefeild.AddExpenseWithTextType('Test2', 'Text', 1);
        await expect(page.getByText('Test1')).toHaveCount(3);
    });
    test('Add Vendor With Boolean', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');
        await customefeild.AddExpenseWitBooleanType(
            'Test1',
            'Boolean',
            1,
            'True'
        );
        await customefeild.AddExpenseWitBooleanType('Test2', 'Boolean', 1);
    });
    test('Add Vendor With Number type', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');
        await customefeild.AddExpenseWithTextType('Number1', 'Number', 1, 123);

        await expect(page.getByText('Number1')).toHaveCount(1);
    });
    test('Add Vendor With TextArea', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');
        await customefeild.AddExpenseWithTextType(
            'TextArea1',
            'TextArea',
            1,
            'Finops Protol'
        );

        await expect(page.getByText('TextArea1')).toHaveCount(1);
    });
    test('Add VendorWith Date type', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');

        await customefeild.AddExpenseWithDateType('Date1', 'Date', 1);
    });
    // test('Add Expense with choice type without choice feild', async ({
    //     page,
    // }) => {
    //     const signin = new SignInHelper(page);
    //     await signin.init();
    //     const username = 'newtestauto@company.com';
    //     const password = '123456';
    //     await signin.checkDashboard({
    //         username: username,
    //         password: password,
    //     });

    //     const customefeild = new CustofeildHelper(page);
    //     await customefeild.init();
    //     await customefeild.AddExpenseWithNumberCheckchoice(
    //         'Choice1',
    //         'Choice',
    //         1,
    //         ['', '']
    //     );
    //     expect(await customefeild.errorMessage()).toBe(
    //         'Choice List is required'
    //     );
    // });
    test('Change Status', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');

        await customefeild.ChangeStatus();
    });
    test('Change Mendatory', async ({ page }) => {
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

        await customefeild.clickExpenseTab('Vendor');

        await customefeild.ChangeMendatory();
    });
    test('Check Edit Link', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');

        await customefeild.CheckEdit();

        await expect(page.getByText('Edit Vendor Custom Field')).toHaveCount(1);
    });
    test('Change Name and Priority', async ({ page }) => {
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
        await customefeild.clickExpenseTab('Vendor');

        await customefeild.changeNameORPriority(
            'Number1',
            'Number',
            1,
            'Number4',
            2
        );

        await expect(page.getByText('Number4')).toHaveCount(1);
    });
});
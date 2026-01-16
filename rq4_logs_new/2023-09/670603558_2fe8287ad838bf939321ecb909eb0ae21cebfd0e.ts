import { TEST_URL } from '@/constants/api.constants';
import { PROCESS_TEST } from '@/fixtures';
import { ExpenseHelper } from '@/helpers/ExpenseHelper/expense.helper';
import {
    ApprovalToggleHelper,
    ApprovalWorkflowsTab,
    FinOpsVerificationHelper,
    PaymentVerificationHelper,
    SavedExpenseCreation,
} from '@/helpers/ExpenseHelper/savedExpense.helper';
import { SignInHelper } from '@/helpers/SigninHelper/signIn.helper';
import { generateRandomNumber } from '@/utils/common.utils';
import chalk from 'chalk';

const { expect, describe } = PROCESS_TEST;
describe.configure({ mode: 'serial' });
describe('FinOps_ExpenseCreation - Expense Creation', () => {
    const BusinessInfo = {
        to: 'Hidesign India Pvt Ltd',
        from: 'Adidas India Marketing Private Limited',
    };
    const EXPENSEDETAILS = {
        invoice: ' inv' + generateRandomNumber(),
        amount: 10000,
        taxable_amount: 10000,
        // expense_head: 'Stationary',
        pay_to: 'Vendor',
        desc: 'Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. N',
    };

    PROCESS_TEST(
        'TECF001 - without poc, department, expense head ',
        async ({ page }) => {
            const expense = new ExpenseHelper(page);
            const toggleHelper = new ApprovalToggleHelper(page);
            const savedExpensePage = new SavedExpenseCreation(page);
            await toggleHelper.gotoExpenseApproval();
            await toggleHelper.allInactive();
            await expense.init();

            await PROCESS_TEST.step('Fill Expense Details', async () => {
                await expense.addDocument();
                await expense.fillBusinessDetails([BusinessInfo]);
                await expense.fillExpenses([EXPENSEDETAILS]);
            });

            await PROCESS_TEST.step('Fill Tax ', async () => {
                await expense.addTaxesData([
                    {
                        gst: '12%',
                        cess: '200',
                        tds: 'Salaries', //takes from second row
                        tcs: '12',
                    },
                ]);
            });
            await PROCESS_TEST.step('Remove default input value', async () => {
                await expense.removeDefaultInputValue('poc');
                await expense.removeDefaultInputValue('department');
            });
            // await page.getByRole('button', { name: 'Save' }).click();
            await expense.clickButton('Save');

            await PROCESS_TEST.step(
                'Check Saved and Party Status',
                async () => {
                    await savedExpensePage.notification.checkToastSuccess(
                        'Invoice raised successfully.'
                    );
                    // expect(
                    //     await savedExpensePage.toastMessage(),
                    //     chalk.red('Invoice not raised.')
                    // ).toBe('Invoice raised successfully.');
                    expect(
                        await savedExpensePage.checkPartyStatus(),
                        chalk.red('Party Status not open.')
                    ).toBe('Open');
                }
            );

            await PROCESS_TEST.step('Expense Status', async () => {
                expect(
                    await savedExpensePage.expenseStatusSuccess('verification'),
                    chalk.red('Verification Status check')
                ).toBe(false);
                expect(
                    await savedExpensePage.expenseStatusSuccess('finops'),
                    chalk.red('FinOps status check')
                ).toBe(false);
                expect(
                    await savedExpensePage.expenseStatusSuccess('payment'),
                    chalk.red('Payment Status check')
                ).toBe(false);
            });

            await PROCESS_TEST.step('Check Business and Vendor', async () => {
                expect(
                    await savedExpensePage.checkExpenseTo(),
                    chalk.red('Check To match')
                ).toBe(BusinessInfo.to);
                expect(
                    await savedExpensePage.checkExpenseFrom(),
                    chalk.red('Check from match')
                ).toBe(BusinessInfo.from);
            });
        }
    );

    PROCESS_TEST('TECF002 - creation with poc', async ({ page }) => {
        // const tabHelper = new TabHelper(page);
        const expense = new ExpenseHelper(page);
        const toggleHelper = new ApprovalToggleHelper(page);
        await toggleHelper.gotoExpenseApproval();
        await toggleHelper.allInactive();
        await expense.init();

        await expense.addDocument();
        await PROCESS_TEST.step('TECF002', async () => {
            await expense.fillBusinessDetails([
                {
                    to: 'Hidesign India Pvt Ltd',
                    from: 'Adidas India Marketing Private Limited',
                },
            ]);
            await expense.fillExpenses([
                {
                    invoice: ' inv' + generateRandomNumber(),
                    amount: 10000,
                    taxable_amount: 10000,
                    // department: 'Hr',
                    // expense_head: 'Rent',
                    poc: 'Abhishek',
                    pay_to: 'Vendor',
                    desc: 'Dummy Text',
                },
            ]);
        });
        await PROCESS_TEST.step('Add Taxes', async () => {
            await expense.addTaxesData([
                {
                    gst: '8%',
                    cess: '250',
                    tds: 'Cash withdrawal exceeding ',
                    tcs: '20',
                },
            ]);
        });
        await PROCESS_TEST.step('Remove default input value', async () => {
            await expense.removeDefaultInputValue('department');
        });
        await expense.clickButton('Save');

        const savedExpensePage = new SavedExpenseCreation(page);

        await PROCESS_TEST.step(
            'Check Saved and Party Status with poc',
            async () => {
                await savedExpensePage.notification.checkToastSuccess(
                    'Invoice raised successfully.'
                );
                // expect(
                //     await savedExpensePage.toastMessage(),
                //     chalk.red('Toast message match')
                // ).toBe('Invoice raised successfully.');
                expect(
                    await savedExpensePage.checkPartyStatus(),
                    chalk.red('Check party status match')
                ).toBe('Submitted');
            }
        );

        await PROCESS_TEST.step('Check Approval Flows', async () => {
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');

            const verificationFlows = new ApprovalWorkflowsTab(page);
            await verificationFlows.checkLevel();
            await verificationFlows.checkUser();
            await verificationFlows.checkEmail();
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                ),
                chalk.red('Verification Approvals match')
            ).toBe('Pending Approval');
        });
    });

    PROCESS_TEST('TECF003 - approval with poc', async ({ page }) => {
        // const tabHelper = new TabHelper(page);
        const expense = new ExpenseHelper(page);
        const signIn = new SignInHelper(page);
        const verificationFlows = new ApprovalWorkflowsTab(page);
        const toggleHelper = new ApprovalToggleHelper(page);
        await toggleHelper.gotoExpenseApproval();
        await toggleHelper.allInactive();
        await expense.init();

        await expense.addDocument();
        await PROCESS_TEST.step('Fill Expense', async () => {
            await expense.fillBusinessDetails([BusinessInfo]);
            await expense.fillExpenses([EXPENSEDETAILS]);
        });
        // await expense.checkDepartmentFetch();
        await PROCESS_TEST.step('Add Taxes', async () => {
            await expense.addTaxesData([
                {
                    gst: '5%',
                    cess: '250',
                    tds: 'Cash withdrawal exceeding ',
                    tcs: '20',
                },
            ]);
        });
        // await PROCESS_TEST.step('Remove default input value', async () => {
        //     await expense.removeDefaultInputValue('department');
        // });
        await expense.clickButton('Save');

        const savedExpensePage = new SavedExpenseCreation(page);

        await PROCESS_TEST.step(
            'Check Saved and Party Status with poc',
            async () => {
                await savedExpensePage.notification.checkToastSuccess(
                    'Invoice raised successfully.'
                );
                // expect(
                //     await savedExpensePage.toastMessage(),
                //     chalk.red('Toast message match')
                // ).toBe('Invoice raised successfully.');
                expect(
                    await savedExpensePage.checkPartyStatus(),
                    chalk.red('Check party status')
                ).toBe('Submitted');
            }
        );

        await PROCESS_TEST.step('Check Approval Flows', async () => {
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');

            await verificationFlows.checkLevel();
            await verificationFlows.checkUser();
            await verificationFlows.checkEmail();
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                ),
                chalk.red('Verification Approval match')
            ).toBe('Pending Approval');
        });

        await PROCESS_TEST.step('Expense Approve', async () => {
            const pocEmail = await verificationFlows.checkEmail();
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await page.waitForLoadState('domcontentloaded');
            await signIn.signInPage(pocEmail, '1234567');
            // await page.waitForSelector('//div[@role="dialog"]', {
            //     state: 'attached',
            // });
            await page.getByText('New Test Auto').click();
            await page.waitForURL(TEST_URL + '/e/e');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.clickApprove([
                {
                    department: 'Sales',
                    expense_head: 'Foods & Accommodations',
                    comment: 'Expense approval with POC',
                },
            ]);
            // await tabHelper.clickTab('Approval Workflows');
            await page
                .locator('//button[@role="tab"]')
                .getByText('Approval Workflows')
                .click();
            await page.waitForTimeout(1000);
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                ),
                chalk.red('Verification Approval match')
            ).toBe('Approved');
        });
        await PROCESS_TEST.step('Level Status in FinOps', async () => {
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await signIn.signInPage('newtestauto@company.com', '123456');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');
            expect(
                await verificationFlows.checkByFinOpsAdmin(
                    'Verification Approvals'
                ),
                chalk.red('Verification approval match')
            ).toBe('Approved');
        });

        await PROCESS_TEST.step('Check Expense Status', async () => {
            expect(
                await savedExpensePage.expenseStatusSuccess('verification'),
                chalk.red('Verification Status check')
            ).toBe(true);

            expect(
                await savedExpensePage.expenseStatusSuccess('finops'),
                chalk.red('FinOps Status check')
            ).toBe(true);
            expect(
                await savedExpensePage.expenseStatusSuccess('payment'),
                chalk.red('Payment status check')
            ).toBe(true);
        });

        await PROCESS_TEST.step('Check Business and Vendor', async () => {
            expect(
                await savedExpensePage.checkExpenseTo(),
                chalk.red('Client name match')
            ).toBe(BusinessInfo.to);
            expect(
                await savedExpensePage.checkExpenseFrom(),
                chalk.red('Vendor name match')
            ).toBe(BusinessInfo.from);
            await page.waitForTimeout(1000);
        });
    });

    PROCESS_TEST('TECF004 - approval with all details', async ({ page }) => {
        // const tabHelper = new TabHelper(page);
        const expense = new ExpenseHelper(page);
        const verificationFlows = new ApprovalWorkflowsTab(page);
        const signIn = new SignInHelper(page);

        const toggleHelper = new ApprovalToggleHelper(page);
        await toggleHelper.gotoExpenseApproval();
        await toggleHelper.allInactive();

        await expense.init();

        await expense.addDocument();
        await PROCESS_TEST.step('Fill Expense', async () => {
            await expense.fillBusinessDetails([
                {
                    to: 'Hidesign India Pvt Ltd',
                    from: 'Adidas India Marketing Private Limited',
                },
            ]);
            await expense.fillExpenses([
                {
                    invoice: ' inv' + generateRandomNumber(),
                    amount: 10000,
                    taxable_amount: 10000,
                    department: 'Sales',
                    expense_head: 'Foods & Accommodations',
                    poc: 'Abhishek',
                    pay_to: 'Vendor',
                    desc: 'Dummy Text',
                },
            ]);
        });
        await PROCESS_TEST.step('Add Taxes', async () => {
            await expense.addTaxesData([
                {
                    gst: '5%',
                    cess: '250',
                    tds: 'Cash withdrawal exceeding ',
                    tcs: '20',
                },
            ]);
            await expense.clickButton('Save');
        });

        const savedExpensePage = new SavedExpenseCreation(page);

        await PROCESS_TEST.step(
            'Check Saved and Party Status with poc',
            async () => {
                await savedExpensePage.notification.checkToastSuccess(
                    'Invoice raised successfully.'
                );
                // expect(await savedExpensePage.toastMessage()).toBe(
                //     'Invoice raised successfully.'
                // );
                expect(await savedExpensePage.checkPartyStatus()).toBe(
                    'Submitted'
                );
            }
        );

        await PROCESS_TEST.step('Check Approval Flows', async () => {
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');

            await verificationFlows.checkLevel();
            await verificationFlows.checkUser();
            await verificationFlows.checkEmail();
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                )
            ).toBe('Pending Approval');
        });

        await PROCESS_TEST.step('POC Approval', async () => {
            const pocEmail = await verificationFlows.checkEmail();
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await page.waitForLoadState('networkidle');
            await page.waitForLoadState('domcontentloaded');
            await signIn.signInPage(pocEmail, '1234567');
            // await page.waitForSelector('//div[@role="dialog"]', {
            //     state: 'attached',
            // });
            await page.getByText('New Test Auto').click();
            await page.waitForURL(TEST_URL + '/e/e');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.clickApprove();
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');
            await page.waitForTimeout(1000);
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                )
            ).toBe('Approved');
        });
        await PROCESS_TEST.step('Level Status in FinOps', async () => {
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await page.waitForLoadState('networkidle');
            await page.waitForLoadState('domcontentloaded');
            await signIn.signInPage('newtestauto@company.com', '123456');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');
            await page.waitForTimeout(1500);
            expect(
                await verificationFlows.checkByFinOpsAdmin(
                    'Verification Approvals'
                )
            ).toBe('Approved');
            expect(
                await savedExpensePage.expenseStatusSuccess('verification')
            ).toBe(true);

            expect(await savedExpensePage.expenseStatusSuccess('finops')).toBe(
                true
            );
            expect(await savedExpensePage.expenseStatusSuccess('payment')).toBe(
                true
            );
        });
    });

    PROCESS_TEST('TECF005 - approval without comment ', async ({ page }) => {
        // const tabHelper = new TabHelper(page);
        const expense = new ExpenseHelper(page);
        const verificationFlows = new ApprovalWorkflowsTab(page);
        const signIn = new SignInHelper(page);

        const toggleHelper = new ApprovalToggleHelper(page);
        await toggleHelper.gotoExpenseApproval();
        await toggleHelper.allInactive();

        await expense.init();

        await expense.addDocument();

        await PROCESS_TEST.step('Fill Expense', async () => {
            await expense.fillBusinessDetails([
                {
                    to: 'Hidesign India Pvt Ltd',
                    from: 'Adidas India Marketing Private Limited',
                },
            ]);
            await expense.fillExpenses([
                {
                    invoice: ' inv' + generateRandomNumber(),
                    amount: 10000,
                    taxable_amount: 10000,
                    department: 'Sales',
                    expense_head: 'Foods & Accommodations',
                    poc: 'Abhishek',
                    pay_to: 'Vendor',
                    desc: 'Dummy Text',
                },
            ]);
        });
        await PROCESS_TEST.step('Add Taxes', async () => {
            await expense.addTaxesData([
                {
                    gst: '5%',
                    cess: '250',
                    tds: 'Cash withdrawal exceeding ',
                    tcs: '20',
                },
            ]);
            await expense.clickButton('Save');
        });

        const savedExpensePage = new SavedExpenseCreation(page);

        await PROCESS_TEST.step(
            'Check Saved and Party Status with poc',
            async () => {
                await savedExpensePage.notification.checkToastSuccess(
                    'Invoice raised successfully.'
                );
                // expect(
                //     await savedExpensePage.toastMessage(),
                //     chalk.red('ToastMessage match')
                // ).toBe('Invoice raised successfully.');
                expect(
                    await savedExpensePage.checkPartyStatus(),
                    chalk.red('ToastMessage match')
                ).toBe('Submitted');
            }
        );

        await PROCESS_TEST.step('Check Approval Flows', async () => {
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');

            await verificationFlows.checkLevel();
            await verificationFlows.checkUser();
            await verificationFlows.checkEmail();
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                ),
                chalk.red('Verification Approval match')
            ).toBe('Pending Approval');
        });

        await PROCESS_TEST.step(
            'Expense Approve and check status',
            async () => {
                const pocEmail = await verificationFlows.checkEmail();
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage(pocEmail, '1234567');
                await page.getByText('New Test Auto').click();
                await page.waitForURL(TEST_URL + '/e/e');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));
                await verificationFlows.clickApproveWithoutComment();
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                expect(
                    await verificationFlows.checkApprovalStatus(
                        'Verification Approvals'
                    ),
                    chalk.red('Verification Approval match')
                ).toBe('Approved');
            }
        );

        await PROCESS_TEST.step('Level Status in FinOps', async () => {
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await page.waitForLoadState('networkidle');
            await page.waitForLoadState('domcontentloaded');
            await signIn.signInPage('newtestauto@company.com', '123456');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');
            expect(
                await verificationFlows.checkByFinOpsAdmin(
                    'Verification Approvals'
                ),
                chalk.red('Verification Approval match')
            ).toBe('Approved');
            expect(
                await savedExpensePage.expenseStatusSuccess('verification'),
                chalk.red('Verification Status check')
            ).toBe(true);

            expect(
                await savedExpensePage.expenseStatusSuccess('finops'),
                chalk.red('Verification Status check')
            ).toBe(true);
            expect(
                await savedExpensePage.expenseStatusSuccess('payment'),
                chalk.red('Payment Approval match')
            ).toBe(true);
        });
    });

    PROCESS_TEST('TECF006 - expense rejection', async ({ page }) => {
        // const tabHelper = new TabHelper(page);
        const expense = new ExpenseHelper(page);
        const signIn = new SignInHelper(page);
        const verificationFlows = new ApprovalWorkflowsTab(page);

        const toggleHelper = new ApprovalToggleHelper(page);
        await toggleHelper.gotoExpenseApproval();
        await toggleHelper.allInactive();

        await expense.init();
        await expense.addDocument();
        await PROCESS_TEST.step('Fill Expense', async () => {
            await expense.fillBusinessDetails([
                {
                    to: 'Hidesign India Pvt Ltd',
                    from: 'Adidas India Marketing Private Limited',
                },
            ]);
            await expense.fillExpenses([
                {
                    invoice: ' inv' + generateRandomNumber(),
                    amount: 10000,
                    taxable_amount: 10000,
                    department: 'Sales',
                    expense_head: 'Foods & Accommodations',
                    poc: 'Abhishek',
                    pay_to: 'Vendor',
                    desc: 'Dummy Text',
                },
            ]);
        });
        await PROCESS_TEST.step('Add Taxes', async () => {
            await expense.addTaxesData([
                {
                    gst: '5%',
                    cess: '250',
                    tds: 'Cash withdrawal exceeding ',
                    tcs: '20',
                },
            ]);
            await expense.clickButton('Save');
        });

        const savedExpensePage = new SavedExpenseCreation(page);

        await PROCESS_TEST.step(
            'Check Saved and Party Status with poc',
            async () => {
                await savedExpensePage.notification.checkToastSuccess(
                    'Invoice raised successfully.'
                );
                // expect(
                //     await savedExpensePage.toastMessage(),
                //     chalk.red('ToastMessage match')
                // ).toBe('Invoice raised successfully.');
                expect(
                    await savedExpensePage.checkPartyStatus(),
                    chalk.red('Check party status match')
                ).toBe('Submitted');
            }
        );

        await PROCESS_TEST.step('Check Approval Flows', async () => {
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');

            await verificationFlows.checkLevel();
            await verificationFlows.checkUser();
            await verificationFlows.checkEmail();
            await page.waitForTimeout(1000);
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                ),
                chalk.red('Verification approval match')
            ).toBe('Pending Approval');
        });

        await PROCESS_TEST.step('Expense Approval Reject', async () => {
            const pocEmail = await verificationFlows.checkEmail();
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await page.waitForLoadState('networkidle');
            await page.waitForLoadState('domcontentloaded');
            await signIn.signInPage(pocEmail, '1234567');
            // await page.waitForSelector('//div[@role="dialog"]', {
            //     state: 'attached',
            // });
            await page.getByText('New Test Auto').click();
            await page.waitForURL(TEST_URL + '/e/e');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.clickReject();
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');
            await page.waitForTimeout(1000);
            expect(
                await verificationFlows.checkApprovalStatus(
                    'Verification Approvals'
                ),
                chalk.red('Verification Approval check')
            ).toBe('Rejected');
        });

        await PROCESS_TEST.step('Level Status in FinOps', async () => {
            const expData = await verificationFlows.getExpData();
            await savedExpensePage.logOut();
            await page.waitForLoadState('networkidle');
            await page.waitForLoadState('domcontentloaded');
            await signIn.signInPage('newtestauto@company.com', '123456');
            await page.waitForURL(TEST_URL + '/e/f');
            await savedExpensePage.clickLink('Expenses');
            await savedExpensePage.clickLink(expData.slice(1));
            await savedExpensePage.tabHelper.clickTab('Approval Workflows');
            await page.waitForTimeout(1000);
            expect(
                await verificationFlows.checkByFinOpsAdmin(
                    'Verification Approvals'
                ),
                chalk.red('Verification approval match')
            ).toBe('Rejected');
            await PROCESS_TEST.step('Check Expense Status', async () => {
                expect(
                    await savedExpensePage.expenseStatusSuccess('verification'),
                    chalk.red('Verification Status check')
                ).toBe(false);

                expect(
                    await savedExpensePage.expenseStatusSuccess('finops'),
                    chalk.red('FinOps Status check')
                ).toBe(false);
                expect(
                    await savedExpensePage.expenseStatusSuccess('payment'),
                    chalk.red('Payment Status check')
                ).toBe(false);
            });
        });
    });

    PROCESS_TEST(
        'TECF007 - approval by individual and auto',
        async ({ page }) => {
            // const tabHelper = new TabHelper(page);
            const signIn = new SignInHelper(page);
            const expense = new ExpenseHelper(page);
            const verificationFlows = new ApprovalWorkflowsTab(page);
            const finOpsFlows = new FinOpsVerificationHelper(page);
            const paymentFlows = new PaymentVerificationHelper(page);

            const toggleHelper = new ApprovalToggleHelper(page);

            // toggle workflow options for expense approvals
            await toggleHelper.gotoExpenseApproval();
            await toggleHelper.toggleOption(
                'Amount >1000 and Auto Approve',
                'Active'
            );
            await toggleHelper.toggleOption(
                'Category,Amount & Department Manager',
                'Inactive'
            );
            await toggleHelper.gotoTab('Finops');
            await toggleHelper.toggleOption(
                'Department and auto Approve',
                'Inactive'
            );
            await toggleHelper.toggleOption(
                'Amount >1000 and Individual',
                'Active'
            );
            await toggleHelper.gotoTab('Payment');
            await toggleHelper.toggleOption(
                'Amount >=5000 and Individual',
                'Inactive'
            );
            await toggleHelper.toggleOption(
                'Amount >1000 and Individual',
                'Active'
            );

            await expense.init();

            await expense.addDocument();

            await PROCESS_TEST.step('Fill Expense', async () => {
                await expense.fillBusinessDetails([BusinessInfo]);
                await expense.fillExpenses([EXPENSEDETAILS]);
            });
            await PROCESS_TEST.step('Add Taxes', async () => {
                await expense.addTaxesData([
                    {
                        gst: '5%',
                        cess: '250',
                        tds: 'Cash withdrawal exceeding ',
                        tcs: '20',
                    },
                ]);
                await expense.clickButton('Save');
            });

            const savedExpensePage = new SavedExpenseCreation(page);

            await PROCESS_TEST.step(
                'Check Saved and Party Status with poc',
                async () => {
                    await savedExpensePage.notification.checkToastSuccess(
                        'Invoice raised successfully.'
                    );
                    // expect(
                    //     await savedExpensePage.toastMessage(),
                    //     chalk.red('Toast message match')
                    // ).toBe('Invoice raised successfully.');

                    expect(
                        await savedExpensePage.checkPartyStatus(),
                        chalk.red('Check party status')
                    ).toBe('Submitted');
                }
            );

            await PROCESS_TEST.step('Check Approval Flows', async () => {
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');

                await verificationFlows.checkLevel();
                await verificationFlows.checkUser();
                await verificationFlows.checkEmail();
                expect(
                    await verificationFlows.checkApprovalStatus(
                        'Verification Approvals'
                    ),
                    chalk.red('Verification approval match')
                ).toBe('Pending Approval');
            });

            await PROCESS_TEST.step('POC Approval', async () => {
                const pocEmail = await verificationFlows.checkEmail();
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage(pocEmail, '1234567');
                await page.getByText('New Test Auto').click();
                await page.waitForURL(TEST_URL + '/e/e');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));
                await savedExpensePage.clickApprove();
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await page.waitForTimeout(1000);
                expect(
                    await verificationFlows.checkApprovalStatus(
                        'Verification Approvals'
                    ),
                    chalk.red('Verification approval match')
                ).toBe('Approved');
            });

            await PROCESS_TEST.step('Level Status in FinOps', async () => {
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage('newtestauto@company.com', '123456');
                await page.waitForURL(TEST_URL + '/e/f');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await verificationFlows.checkApprovalByFinOps();
            });
            await PROCESS_TEST.step(
                'Check pending flows and party status in finops',
                async () => {
                    await verificationFlows.checkPendingFlows();
                }
            );

            await PROCESS_TEST.step('Check finOpsFlows Details', async () => {
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await finOpsFlows.getLevelName();
                await finOpsFlows.getLevelStatus();
                await finOpsFlows.getEmailName();
                await finOpsFlows.getFinopsEmail();
            });

            await PROCESS_TEST.step('FinOps Approval', async () => {
                const finOpsEmail = await finOpsFlows.getFinopsEmail();
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage(finOpsEmail, '1234567');
                await page.getByText('New Test Auto').click();
                await page.waitForURL(TEST_URL + '/e/e');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));
                await savedExpensePage.clickApprove();

                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await page.waitForLoadState('networkidle');
                expect(
                    await finOpsFlows.getLevelStatus(),
                    chalk.red('FinOps level status match')
                ).toBe('Approved');
            });

            await PROCESS_TEST.step('Payment Approval', async () => {
                const paymentEmail = await paymentFlows.getPaymentEmail();
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage(paymentEmail, '1234567');
                await page.getByText('New Test Auto').click();
                await page.waitForURL(TEST_URL + '/e/e');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));

                await savedExpensePage.clickApprove();

                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await page.waitForTimeout(1000);
                expect(
                    await paymentFlows.getLevelStatus(),
                    chalk.red('Payment level status match')
                ).toBe('Approved');
            });
            await PROCESS_TEST.step('Check Business and Vendor', async () => {
                expect(
                    await savedExpensePage.checkExpenseTo(),
                    chalk.red('Check To Expense Details match')
                ).toBe(BusinessInfo.to);
                expect(
                    await savedExpensePage.checkExpenseFrom(),
                    chalk.red('Vendor name match')
                ).toBe(BusinessInfo.from);
                await page.waitForTimeout(1000);
            });
        }
    );

    PROCESS_TEST(
        'TECF008 - approval by department manager',
        async ({ page }) => {
            // const tabHelper = new TabHelper(page);
            const signIn = new SignInHelper(page);
            const expense = new ExpenseHelper(page);
            const verificationFlows = new ApprovalWorkflowsTab(page);
            const finOpsFlows = new FinOpsVerificationHelper(page);
            const paymentFlows = new PaymentVerificationHelper(page);
            const toggleHelper = new ApprovalToggleHelper(page);

            // toggle workflow options for expense approvals
            await toggleHelper.gotoExpenseApproval();
            await toggleHelper.toggleOption(
                'Amount >1000 and Auto Approve',
                'Inactive'
            );
            await toggleHelper.toggleOption(
                'Category,Amount & Department Manager',
                'Active'
            );
            await toggleHelper.gotoTab('Finops');
            await toggleHelper.toggleOption(
                'Department and auto Approve',
                'Active'
            );
            await toggleHelper.toggleOption(
                'Amount >1000 and Individual',
                'Inactive'
            );
            await toggleHelper.gotoTab('Payment');
            await toggleHelper.toggleOption(
                'Amount >=5000 and Individual',
                'Active'
            );
            await toggleHelper.toggleOption(
                'Amount >1000 and Individual',
                'Inactive'
            );

            await expense.init();

            await expense.addDocument();

            await PROCESS_TEST.step('Fill Expense', async () => {
                await expense.fillBusinessDetails([BusinessInfo]);
                await expense.fillExpenses([EXPENSEDETAILS]);
            });

            // await PROCESS_TEST.step('Add Taxes', async () => {
            //     await expense.addTaxesData([
            //         {
            //             gst: '5%',
            //             cess: '250',
            //             tds: 'Cash withdrawal exceeding ',
            //             tcs: '20',
            //         },
            //     ]);
            //     await expense.clickButton('Save');
            // });
            await expense.clickButton('Save');

            const savedExpensePage = new SavedExpenseCreation(page);

            await PROCESS_TEST.step(
                'Check Saved and Party Status with poc',
                async () => {
                    await savedExpensePage.notification.checkToastSuccess(
                        'Invoice raised successfully.'
                    );
                    // expect(
                    //     await savedExpensePage.toastMessage(),
                    //     chalk.red('Toast Message match')
                    // ).toBe('Invoice raised successfully.');

                    expect(
                        await savedExpensePage.checkPartyStatus(),
                        chalk.red('Party Status match')
                    ).toBe('Submitted');
                }
            );

            await PROCESS_TEST.step('Check Approval Flows', async () => {
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');

                await verificationFlows.checkLevel();
                await verificationFlows.checkUser();
                await verificationFlows.checkEmail();
                expect(
                    await verificationFlows.checkApprovalStatus(
                        'Verification Approvals'
                    ),
                    chalk.red('Approval Status match')
                ).toBe('Pending Approval');
            });

            await PROCESS_TEST.step('POC Approval', async () => {
                const pocEmail = await verificationFlows.checkEmail();
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage(pocEmail, '1234567');
                await page.getByText('New Test Auto').click();
                await page.waitForURL(TEST_URL + '/e/e');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));
                await savedExpensePage.clickApprove();
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await page.waitForTimeout(1000);
                expect(
                    await verificationFlows.checkApprovalStatus(
                        'Verification Approvals'
                    ),
                    chalk.red('Verification Approval match')
                ).toBe('Approved');
            });

            await PROCESS_TEST.step('Level Status in FinOps', async () => {
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('networkidle');
                await page.waitForLoadState('domcontentloaded');
                await signIn.signInPage('newtestauto@company.com', '123456');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));
                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await page.waitForTimeout(1000);
                expect(
                    await verificationFlows.checkByFinOpsAdmin(
                        'Verification Approvals'
                    ),
                    chalk.red('Verification Approval match')
                ).toBe('Approved');
                // await verificationFlows.checkApprovalByFinOps();
            });

            await PROCESS_TEST.step(
                'Level Two Verification Flows',
                async () => {
                    const nextEmail = await verificationFlows.nextApprovalFlows(
                        'Verification Approvals'
                    );
                    console.log('Next Email: ', nextEmail);
                    const expData = await verificationFlows.getExpData();
                    await savedExpensePage.logOut();
                    await page.waitForLoadState('networkidle');
                    await page.waitForLoadState('domcontentloaded');
                    await signIn.signInPage(nextEmail, '1234567');
                    await page.getByText('New Test Auto').click();
                    await page.waitForURL(TEST_URL + '/e/e');
                    await savedExpensePage.clickLink('Expenses');
                    await savedExpensePage.clickLink(expData.slice(1));
                    await savedExpensePage.clickApprove();
                    await page.waitForTimeout(1000);
                    await savedExpensePage.tabHelper.clickTab(
                        'Approval Workflows'
                    );
                    await verificationFlows.checkManagerApproval();
                }
            );

            await PROCESS_TEST.step('Payment Approval', async () => {
                await page.waitForTimeout(1000);
                const paymentEmail = await paymentFlows.getPaymentEmail();
                const expData = await verificationFlows.getExpData();
                await savedExpensePage.logOut();
                await page.waitForLoadState('load');
                await signIn.signInPage(paymentEmail, '1234567');
                await page.getByText('New Test Auto').click();
                await page.waitForURL(TEST_URL + '/e/e');
                await savedExpensePage.clickLink('Expenses');
                await savedExpensePage.clickLink(expData.slice(1));

                await savedExpensePage.clickApprove();

                await savedExpensePage.tabHelper.clickTab('Approval Workflows');
                await page.waitForTimeout(1000);
                expect(
                    await paymentFlows.getLevelStatus(),
                    chalk.red('Level status match')
                ).toBe('Approved');
            });
        }
    );
});
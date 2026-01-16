import { PROCESS_TEST } from '@/fixtures';
import { WorkflowListingHelper } from '@/helpers/WorkflowHelper/workflowListing.helper';
const { expect, describe } = PROCESS_TEST;

describe('TWE001', () => {
    PROCESS_TEST('Expense Approval Workflow', async ({ page }) => {
        const helper = new WorkflowListingHelper(page);
        await helper.init('expense');

        await helper.checkTabExists(['Verifications', 'Finops', 'Payments']);

        PROCESS_TEST.step('Check Payments Tab', async () => {
            await helper.clickTab('Payments');
            await helper.clickTab('Finops');
            await helper.clickTab('Verifications');
        });

        PROCESS_TEST.step('Open Approval Creation', async () => {
            await helper.openApprovalCreationForm('Verifications');
        });
        // Click and check if tab is selected

        await page.waitForTimeout(1000);
    });
});